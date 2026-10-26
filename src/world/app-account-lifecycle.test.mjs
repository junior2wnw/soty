import * as appInvitationCarrier from './app-invitation-carrier.mjs';
import * as nativeVoiceCarrier from './native-voice-carrier.mjs';
import { createNativeVoiceDescriptor } from '../platform/native-voice-descriptor.mjs';
import * as conversationTargets from './conversation-target.mjs';
import * as chatState from './chat-state.mjs';
import * as messenger from './messenger.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';
import * as appLaunch from './app-launch.mjs';
import * as workspaceReturn from './workspace-return.mjs';
import * as appInbox from './app-inbox.mjs';
import * as appSavedState from './app-saved-state.mjs';
import * as fieldContract from '../../modules/field/contract.mjs';
import * as appBootRecovery from './app-boot-recovery.mjs';
import * as appRuntimePhaseTrace from './app-runtime-phase-trace.mjs';
import * as appRuntimeDisplay from './app-runtime-display.mjs';
import * as widgetRuntimeControls from './app-widget-runtime-controls.mjs';
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
import { createOtsovEditorHost } from './otsov-editor-handoff.mjs';
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
const source = await readFile(process.env.SOTY_FIELD_RETURN_SOURCE ?? new URL('./app.ts', import.meta.url), 'utf8');
const compiled = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const appAst = ts.createSourceFile('app.js', compiled, ts.ScriptTarget.Latest, true, ts.ScriptKind.JS);
const appConstructor = appAst.statements.find(node => ts.isClassDeclaration(node) && node.name?.text === 'WorldApplication').members.find(ts.isConstructorDeclaration);
const rootClickHandler = appConstructor.body.statements.find(node => ts.isExpressionStatement(node) && ts.isCallExpression(node.expression)
  && node.expression.expression.getText(appAst) === 'root.addEventListener' && node.expression.arguments[0]?.text === 'click').expression.arguments[1].getText(appAst);
const connectionHandler=appConstructor.body.statements.find(node=>ts.isExpressionStatement(node)&&ts.isBinaryExpression(node.expression)
  &&node.expression.left.getText(appAst)==='this.unsubscribePwa').expression.right.arguments[0].getText(appAst);
const stageSource = await readFile(process.env.SOTY_NEUTRAL_RUNTIME_STAGE_SOURCE ?? new URL('./app-stage.ts', import.meta.url), 'utf8');
const stageCompiled = ts.transpileModule(stageSource, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const dialogsSource = await readFile(new URL('./dialogs.ts', import.meta.url), 'utf8');
const dialogsCompiled = ts.transpileModule(dialogsSource, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const productSource = await readFile(new URL('./product.ts', import.meta.url), 'utf8');
const productCompiled = ts.transpileModule(productSource, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const accountCenterCompiled = ts.transpileModule(await readFile(new URL('./account-center.ts', import.meta.url), 'utf8'),
  { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const originContinuityCompiled = ts.transpileModule(await readFile(new URL('../platform/origin-continuity.ts', import.meta.url), 'utf8'),
  { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const fieldDirectoryCompiled = ts.transpileModule(await readFile(new URL('./field-directory.ts', import.meta.url), 'utf8'),
  { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };
const turn = () => new Promise(resolve => setImmediate(resolve));
const failure = code => Object.assign(new Error(code), { code });
// Inline declarations used by the actual mounted controls; no layout or CSS parsing is simulated.
function stylePort() {
  const values = new Map();
  const style = {
    setProperty(name, value) { if (value == null || value === '') values.delete(name); else values.set(name, String(value)); },
    getPropertyValue(name) { return values.get(name) ?? ''; },
    removeProperty(name) { const previous = this.getPropertyValue(name); values.delete(name); return previous; },
  };
  for (const name of ['left', 'top', 'width', 'height', 'max-width', 'max-height', 'translate', 'z-index']) {
    Object.defineProperty(style, name.replace(/-([a-z])/gu, (_match, letter) => letter.toUpperCase()), {
      enumerable: true, get() { return style.getPropertyValue(name); }, set(value) { style.setProperty(name, value); },
    });
  }
  return style;
}
class ElementPort {
  style = stylePort();
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
  emit(type, event = {}) { for (const fn of [...this.handlers.get(type) || []]) fn(event); }
  dispatchEvent(event) { this.emit(event.type, event); return !event.defaultPrevented; }
  setAttribute(name, value) { this.attributes.set(name, String(value)); }
  removeAttribute(name) { this.attributes.delete(name); }
  getAttribute(name) { return this.attributes.get(name) ?? null; }
  contains(node) { return node === this || this.children.some(child => child instanceof ElementPort && child.contains(node)); }
  focus() { if (this.ownerDocument) this.ownerDocument.activeElement = this; }
  getBoundingClientRect() { return { top: 0, left: 0, right: 800, bottom: 400, width: 800, height: 400 }; }
  getClientRects() { return [this.getBoundingClientRect()]; }
  querySelectorAll(selector) {
    const matches = child => selector === '[data-message-seq]' ? child.dataset.messageSeq !== undefined
      : selector.startsWith('.') ? child.className.split(' ').includes(selector.slice(1)) : child.tagName === selector;
    return this.children.flatMap(child => child instanceof ElementPort ? [...(matches(child) ? [child] : []), ...child.querySelectorAll(selector)] : []);
  }
  closest(selector) { return (selector.startsWith('.') && this.className.split(' ').includes(selector.slice(1))) ? this : this.parent?.closest(selector) ?? null; }
  replaceWith(node) { if (this.parent) { const parent = this.parent, index = parent.children.indexOf(this); parent.children[index] = node; node.parent = parent; this.parent = null; } }
  requestSubmit() { this.emit('submit', { preventDefault() {} }); }
  remove() { this.isConnected = false; if (this.parent) this.parent.children = this.parent.children.filter(node => node !== this); this.parent = null; }
}
test('ElementPort inline styles retain values across CSS names and camelCase without sharing declarations', () => {
  const element = new ElementPort(), sibling = new ElementPort();
  element.style.maxHeight = '120px';
  assert.equal(element.style.getPropertyValue('max-height'), '120px'); assert.equal(sibling.style.maxHeight, '');
  assert.equal(element.style.removeProperty('max-height'), '120px'); assert.equal(element.style.maxHeight, '');
  assert.equal(element.style.removeProperty('max-height'), '');
  element.style.setProperty('max-width', '240px'); assert.equal(element.style.maxWidth, '240px');
  element.style.setProperty('max-width', ''); assert.equal(element.style.maxWidth, '');
  element.style.translate = '8px 0px'; assert.equal(element.style.removeProperty('translate'), '8px 0px');
  element.style.setProperty('--owned-test-value', 'one'); assert.equal(element.style.getPropertyValue('--owned-test-value'), 'one');
  element.style.removeProperty('--owned-test-value'); assert.equal(element.style.getPropertyValue('--owned-test-value'), '');
});

function fixture({ initial = 'account-A', hash = '#mine', coarse = false } = {}) {
  const module = { exports: {} }, location = { hash, href: `https://shell.example/${hash}`, origin: 'https://shell.example' };
  const document = { activeElement: null, visibilityState: 'visible', addEventListener() {}, removeEventListener() {} };
  const timers = new Map(), intervals = new Map(); let timerId = 0;
  const view = Object.assign(new EventTarget(), { location, document, Node: ElementPort, Event, crypto, btoa,
    setTimeout(fn, delay) { const id = ++timerId; timers.set(id, { fn, delay }); return id; }, clearTimeout(id) { timers.delete(id); },
    setInterval(fn, delay) { const id = ++timerId; intervals.set(id, { fn, delay }); return id; }, clearInterval(id) { intervals.delete(id); },
    navigator: { userActivation: { isActive: false } }, matchMedia: () => ({ matches: coarse, addEventListener() {} }) });
  document.defaultView = view;
  view.MutationObserver = class { observe() {} disconnect() {} };
  const discussionCalls = { refresh: 0 }, nativeVoiceBridges = [], settingsMounts = [], settingsCloseRequests = [];
  const makeElement = (tagName, className, label) => Object.assign(new ElementPort(), { ownerDocument: document, textContent: label ?? '', tagName, className: className ?? '',
    ...(tagName === 'iframe' ? { contentWindow: { sent: [], postMessage(data, origin) { this.sent.push({ data, origin }); } } } : {}) });
  const makeButton = (label, _icon, className, callback) => { const value = Object.assign(makeElement('button', className, label), { disabled: false }); if (callback) value.addEventListener('click', callback); return value; };
  const ports = {
    './dom': { el: makeElement, button:makeButton,iconButton:(label,icon,callback)=>makeButton(label,icon,'',callback), emptyState: label => makeElement('div', '', label),
      avatar: label => makeElement('span', 'sw-avatar', label),
      textInput: (value = '', placeholder = '') => Object.assign(makeElement('input'), { value, placeholder }),
      labeledField: (label, control) => { const element = makeElement('label', '', label); element.append(control); return element; } },
    './dialogs': { errorText: error => error.code ?? 'failure' },
    './product': { loadDeskPreferences: accountId => ({ favorites: [`favorite-${accountId}`], recent: [], pinnedApps: [`pin-${accountId}`] }) },
    './hex-field': { createHexFieldState: () => ({ fresh: true }) },
    './app-launch.mjs': appLaunch,
    './workspace-return.mjs': workspaceReturn,
    './app-inbox.mjs': appInbox,
    './app-saved-state.mjs': appSavedState,
    '../../modules/field/contract.mjs': fieldContract,
    './conversation-target.mjs': conversationTargets,
    './chat-state.mjs': chatState,
    './messenger.mjs': messenger,
    './app-boot-recovery.mjs': appBootRecovery,
    './app-runtime-phase-trace.mjs': appRuntimePhaseTrace,
    './app-runtime-display.mjs': appRuntimeDisplay,
    './app-widget-runtime-controls.mjs': widgetRuntimeControls,
    './browser-assist-pilot.mjs': browserAssistPilot,
    './app-audience.mjs': appAudience,
    './app-actions.mjs': appActions,
    './hive-device-bridge.mjs': hiveDeviceBridge,
    './native-voice-carrier.mjs': { mountRootVoiceCarrier(options) {
      const record = { options, disposed: 0 }; nativeVoiceBridges.push(record);
      const dispose = nativeVoiceCarrier.mountRootVoiceCarrier(options);
      return () => { record.disposed++; dispose(); };
    } },
    './app-invitation-carrier.mjs': appInvitationCarrier,
    './app-fullscreen.mjs': appFullscreen,
    './app-deployment.mjs': appDeployment,
    './project-feedback-capture.mjs': projectCapture,
    './project-feedback-picker.mjs': projectPicker,
    './app-project-capture.mjs': appProjectCapture,
    './app-scoped-renewal.mjs': appScopedRenewal,
    './otsov-editor-handoff.mjs': { createOtsovEditorHost },
    './icons': { icon: () => makeElement('svg') },
    './application-card': { appTone: () => 'neutral' },
    './app-saved': { mountAppSaved: () => ({ dispose() {}, async refresh() {} }) },
    './app-settings': { mountAppSettings(options) { settingsMounts.push(options); options.host.append(...(options.aboutActions ?? []), ...(options.supportActions ?? [])); return { dispose() {}, requestClose(opener, afterClose) { settingsCloseRequests.push({ opener, finish: () => options.onClose(afterClose) }); } }; } },
    './app-discussion': { mountAppDiscussion: () => ({ dispose() {}, async refresh() { discussionCalls.refresh++; }, async flush() {}, hasUnsavedChanges: () => !!discussionCalls.unsaved, setVisible() {}, async updateEntry() {}, async updateSelection() {}, focus() {} }) },
  };
  const dialogsModule = { exports: {} };
  vm.runInNewContext(dialogsCompiled, { module: dialogsModule, exports: dialogsModule.exports, require: name => ports[name] ?? {}, crypto: globalThis.crypto });
  ports['./dialogs'].pinDialogSubmit = dialogsModule.exports.pinDialogSubmit;
  for (const [name, code] of [['./account-center', accountCenterCompiled], ['./field-directory', fieldDirectoryCompiled], ['../platform/origin-continuity', originContinuityCompiled]]) {
    const loaded = { exports: {} };
    vm.runInNewContext(code, { module: loaded, exports: loaded.exports, require: name => ports[name] ?? {}, document, location, crypto: globalThis.crypto });
    ports[name] = loaded.exports;
  }
  const productModule = { exports: {} }, preferences = new Map();
  vm.runInNewContext(productCompiled, { module: productModule, exports: productModule.exports, require: name => ports[name] ?? {},
    localStorage: { getItem: key => preferences.get(key) ?? null, setItem: (key, value) => preferences.set(key, value) }, URLSearchParams });
  ports['./product'].saveDeskPreferences = productModule.exports.saveDeskPreferences;
  const stageModule = { exports: {} };
  vm.runInNewContext(stageCompiled, { module: stageModule, exports: stageModule.exports, require: name => ports[name] ?? {}, AbortController, URL, crypto: globalThis.crypto });
  ports['./app-stage'] = stageModule.exports;
  vm.runInNewContext(`${compiled}\nexports.TestController = WorldApplication; exports.bindRootClick = function() { return ${rootClickHandler}; }; exports.bindConnection = function() { return ${connectionHandler}; };`, {
    module, exports: module.exports, require: name => ports[name] ?? {}, location, window: view,
    document, navigator: view.navigator, crypto: globalThis.crypto, Element: ElementPort, HTMLElement: ElementPort, Event, URL, URLSearchParams, setTimeout, clearTimeout, clearInterval,
    setInterval: view.setInterval, matchMedia: view.matchMedia, ResizeObserver: class { observe() {} disconnect() {} },
    history: { replaceState(_state, _title, url) { location.hash = url; }, pushState(_state, _title, url) { location.hash = url; } }, console,
  });
  const Controller = module.exports.TestController, app = Object.create(Controller.prototype);
  let local = initial, online = true, apiError = null;
  const requests = [], personalFrames = [], dialogs = [];
  Object.assign(app, {
    deskAccount: initial, accountGeneration: 1, requestSequence: 0, screenSequence: 1, homeRequest: 0, communityEpoch: 0, communityReadSequence: 0, communityAppliedReadSequence: 0, destroyed: false,
    root: makeElement('div'), main: makeElement('main'), live: makeElement('div'), dialogs: new Set(), avatars: { setContext() {} },
    appSettingsDialog: null, appSettingsRouteClose: null, field: null, homeHandle: null, notesHandle: null, assistantHandle: null, accessHandle: null, appStage: null,
    chatCleanup: null, chatTimer: null, searchTimer: null, toastTimer: null, noteActionPending: false, visibilityOpen: false,
    profile: { profileId: initial, displayName: `private-${initial}` }, communities: [{ communityId: `group-${initial}` }],
    apps: [{ appId: `app-${initial}`, name: `private-${initial}` }], devices: [{ deviceId: `device-${initial}` }], homeNotes: [{ noteId: `note-${initial}` }],
    group: null, selected: null, selectedChat: `chat-${initial}`, groupReturn: 'world', groupTab: 'apps', directThreads: [], directThreadsLoaded: false, directLabels: new Map(), directOpenSequence: 0, directProjectionEpoch: 0,
    applicationDiscussions: [], applicationInboxEntries: [], applicationFieldEntries: [], applicationInboxStatus: 'unknown', applicationInboxSequence: 0,
    applicationInboxNextCursor: null, applicationInboxPages: 1, applicationInboxDirty: false, applicationInboxAttemptedAt: 0,
    applicationInboxPending: false, applicationInboxScheduled: false, applicationReadState: 'synced', messagesView: { query: '', unreadOnly: false, scroll: 0 },
    applicationReturn: null, pendingReturnPresentation: null,
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
  return { app, Controller, view, timers, intervals, location, requests, personalFrames, dialogs, discussionCalls, nativeVoiceBridges, settingsMounts, settingsCloseRequests, ports, makeElement, connectionChanged: module.exports.bindConnection.call(app), rootClick: module.exports.bindRootClick.call(app), mountStage: stageModule.exports.mountAppStage,
    workspacePreferences: productModule.exports, useSavedWorkspace() { ports['./product'].loadDeskPreferences = productModule.exports.loadDeskPreferences; },
    setLocal(value) { local = value; }, setOnline(value) { online = value; }, setApiError(value) { apiError = value; } };
}

test('recent work uses the existing durable navigation fence and rejects a removed route or retired account generation', () => {
  for (const state of ['current', 'removed', 'foreign-route', 'retired']) {
    const f = fixture(), route = 'notes/private-note', opened = []; let finish;
    f.app.desk.recent = [{ route, title: 'Личная записка', symbol: 'note' }];
    f.app.afterNoteSaved = action => { finish = action; };
    f.app.writeRoute = value => opened.push(value); f.app.openRoute = async () => opened.push('opened');
    f.app.openRecentWork(state === 'foreign-route' ? 'notes/another-note' : route);
    assert.deepEqual(opened, [], 'navigation waits for existing durable work');
    if (state === 'removed') f.app.desk.recent = [];
    if (state === 'retired') f.app.accountGeneration += 2; // A -> B -> A cannot revive this callback.
    finish(); assert.deepEqual(opened, state === 'current' ? [route, 'opened'] : [], state);
  }
});

test('cold application inbox uses the single server discovery API and refresh retains loaded pages', async () => {
  const f = fixture(), calls = [], appId = 'app-' + 'a'.repeat(32);
  const makeRow = (id, source = 'known') => ({ appId: id, name: 'Shared app', source,
    entry: { appId: id, domainId: 'dom_' + 'b'.repeat(32), origin: 'https://app.example', path: '/board?q=a%2Bb' },
    conversationId: 'conv_' + id.slice(4), unreadCount: 1, lastMessage: null });
  const first = makeRow(appId), second = makeRow('app-' + 'c'.repeat(32));
  f.app.api = { request: async (op, args) => { calls.push({ op, args });
    assert.equal(op, 'apps.discussion.inbox');
    return { schema: 'soty.app-discussion.inbox.v1', checkedAt: 1, readState: 'synced', resetRequired: false,
      discussions: [args.cursor ? second : first], nextCursor: args.cursor ? null : 'second-page' };
  } };
  await f.app.refreshApplicationInbox();
  assert.equal(calls.length, 1); assert.deepEqual(JSON.parse(JSON.stringify(calls[0].args.entries)), []);
  assert.equal(f.app.applicationDiscussions.length, 1); assert.equal(f.app.applicationInboxNextCursor, 'second-page');
  await f.app.refreshApplicationInbox(true);
  assert.equal(f.app.applicationDiscussions.length, 2); assert.equal(f.app.applicationInboxPages, 2);
  assert.equal(f.app.applicationInboxNextCursor, null);
  await f.app.refreshApplicationInbox();
  assert.equal(calls.length, 4); assert.equal(f.app.applicationDiscussions.length, 2);
  assert.equal(calls[3].args.cursor, 'second-page', 'normal polling refreshes already loaded pages instead of hiding them');
});

test('inbox pagination cannot publish the preceding profile after an account switch', async () => {
  const f = fixture(), held = deferred(); f.app.api = { request: () => held.promise };
  f.app.applicationInboxNextCursor = 'older';
  const pending = f.app.refreshApplicationInbox(true);
  f.app.deskAccount = 'account-B'; f.app.accountGeneration++;
  held.resolve({ schema: 'soty.app-discussion.inbox.v1', discussions: [], checkedAt: 1,
    nextCursor: null, resetRequired: false, readState: 'synced' });
  await pending;
  assert.equal(f.app.applicationInboxNextCursor, 'older');
  assert.equal(f.app.applicationInboxPages, 1, 'stale work cannot mutate pagination for the new account');
});

test('a hidden, replaced or already polling Messages screen issues no extra application inbox calls', async () => {
  const method = appAst.statements.find(node => ts.isClassDeclaration(node) && node.name?.text === 'WorldApplication')
    .members.find(node => node.name?.getText(appAst) === 'renderMessages');
  const poll = method.body.statements.find(node => ts.isVariableStatement(node)
    && node.declarationList.declarations.some(item => item.name.getText(appAst) === 'refreshInbox'))
    .declarationList.declarations.find(item => item.name.getText(appAst) === 'refreshInbox').initializer;
  let active = false, apps = 0, direct = 0, communities = 0;
  const held = deferred(), environment = { exports: {}, active: () => active, refreshing: false, selectedUpdate: 0,
    document: { visibilityState: 'visible' }, refreshDirect() { direct++; } };
  vm.runInNewContext(`exports.make = function () { return ${poll.getText(appAst)}; };`, environment);
  const run = environment.exports.make.call({ refreshApplicationInbox() { apps++; }, communityEpoch: 0, communityReadSequence: 0,
    api: { request() { communities++; return held.promise; } } });
  await run(); assert.equal(apps, 0);
  active = true; environment.document.visibilityState = 'hidden'; await run(); assert.equal(apps, 0);
  environment.document.visibilityState = 'visible'; const pending = run(); await run();
  assert.equal(apps, 1); assert.equal(direct, 1); assert.equal(communities, 1);
  active = false; held.resolve({ communities: [] }); await pending;
  assert.equal(environment.refreshing, false);
});

function feedbackSettingsFixture(availability = { notesCreateEnabled: false, audience: 'https://shell.example', appFeedbackEnabled: true, mcpAudience: 'https://shell.example/mcp' }) {
  const f = fixture(), appId = 'app-' + 'a'.repeat(32), navigation = [];
  f.app.options.accessAvailability = async () => availability;
  f.app.navigateReady = (...args) => navigation.push(args);
  const open = (canEdit = true) => {
    f.app.openAppSettings({ appId, name: 'Selected app', status: 'ready', ownerAccountId: f.app.deskAccount }, undefined, { isCurrent: () => true, resolve: () => null });
    f.settingsMounts.at(-1).onChanged({ app: { id: appId, name: 'Selected app', state: 'enabled', grants: { accountIds: [], communityIds: [] } },
      actions: { canEdit }, source: { hostDeviceId: 'host', deviceName: 'Device', entryPath: '/', observation: { state: 'responding' } },
      addresses: { aliases: [], canonical: null }, publication: { launchPolicy: 'private', activeDomainIds: [] } });
    return f.dialogs.at(-1).body.querySelectorAll('button').find(node => node.textContent === 'Подключить агента к обращениям');
  };
  return { ...f, appId, navigation, open };
}

test('owned app agent entry passes exact account/app through the existing settings close guard', async () => {
  const f = feedbackSettingsFixture(), entry = f.open(); await turn();
  assert.equal(entry.hidden, false); assert.equal(entry.disabled, false);
  entry.emit('click'); assert.equal(f.settingsCloseRequests.length, 1); assert.equal(f.navigation.length, 0);
  assert.equal(f.settingsCloseRequests[0].opener, entry);
  f.settingsCloseRequests[0].finish();
  assert.deepEqual(JSON.parse(JSON.stringify(f.navigation)), [['access', null, { accountId: 'account-A', appId: f.appId, name: 'Selected app' }]]);
});

test('feedback access cannot bypass an unsaved underlying editor after settings close', async () => {
  const f = feedbackSettingsFixture(); let flushed = 0;
  f.app.notesHandle = { hasUnsavedChanges: () => true, async flush() { flushed++; }, dispose() {} };
  const entry = f.open(); await turn(); entry.emit('click'); f.settingsCloseRequests[0].finish(); await turn();
  assert.equal(flushed, 1); assert.equal(f.navigation.length, 0); assert.match(f.app.lastToast, /Сначала сохраните/u);
});

test('actual access navigation and async panel mount receive the one-use exact app preset', async () => {
  const f = feedbackSettingsFixture(), mounts = [];
  f.ports['./access-panel'] = { mountAccessPanel(_host, options) { mounts.push(options); return { dispose() {} }; } };
  f.app.navigateReady = f.Controller.prototype.navigateReady; f.app.renderCurrent = f.Controller.prototype.renderCurrent;
  f.app.preferences = {}; f.app.persist = () => {}; f.app.writeRoute = route => { f.location.hash = '#' + route; f.app.activeRoute = f.location.hash; };
  const entry = f.open(); await turn(); entry.emit('click'); f.settingsCloseRequests[0].finish(); await turn();
  assert.equal(f.app.view, 'access'); assert.equal(mounts.length, 1);
  assert.deepEqual(JSON.parse(JSON.stringify(mounts[0].appFeedback)), { accountId: 'account-A', appId: f.appId, name: 'Selected app' });
  f.app.renderCurrent(); await turn();
  assert.equal(mounts.length, 2); assert.equal(mounts[1].appFeedback, undefined, 'a generic remount cannot resume an old app consent');
});

test('account switch invalidates both deferred availability and a pending settings handoff', async () => {
  const f = feedbackSettingsFixture(), held = deferred(); f.app.options.accessAvailability = () => held.promise;
  const entry = f.open(); assert.equal(entry.disabled, true);
  f.app.deskAccount = 'account-B'; f.app.accountGeneration++;
  held.resolve({ appFeedbackEnabled: true, audience: 'https://shell.example', mcpAudience: 'https://shell.example/mcp' }); await turn();
  assert.equal(entry.disabled, true); entry.emit('click'); assert.equal(f.settingsCloseRequests.length, 0);
  const next = feedbackSettingsFixture(), nextEntry = next.open(); await turn(); nextEntry.emit('click');
  next.app.deskAccount = 'account-B'; next.app.accountGeneration++; next.settingsCloseRequests[0].finish();
  assert.equal(next.navigation.length, 0);
});

test('old backend explains disabled feedback entry and nonowners do not get an entry', async () => {
  const f = feedbackSettingsFixture({ notesCreateEnabled: true, audience: 'https://shell.example' }), entry = f.open(); await turn();
  assert.equal(entry.disabled, true); assert.equal(entry.hidden, false);
  assert.ok(f.dialogs.at(-1).body.children.some(node => !node.hidden && node.textContent.includes('Сервер пока не поддерживает')));
  entry.emit('click'); assert.equal(f.settingsCloseRequests.length, 0);
  const other = feedbackSettingsFixture(), hiddenEntry = other.open(false); await turn();
  assert.equal(hiddenEntry.hidden, true); hiddenEntry.emit('click'); assert.equal(other.settingsCloseRequests.length, 0);
});

test('four persistent workspace destinations use the real dirty-screen guard and reflect the selected view', async () => {
  const f = fixture(), gate = deferred(); let dirty = true;
  Object.assign(f.app, { sidebar: new ElementPort(), header: new ElementPort(), mobileNav: new ElementPort(), fieldNavigation: new ElementPort(), updateNavUnread() {} });
  f.app.notesHandle = { hasUnsavedChanges: () => dirty, flush: () => gate.promise };
  const selected = []; f.app.navigateReady = view => selected.push(view);
  f.Controller.prototype.renderNavigation.call(f.app);
  const [mine, messages, search, profile] = f.app.fieldNavigation.children;
  assert.deepEqual(f.app.fieldNavigation.children.map(node => node.dataset.navView), ['mine', 'messages', 'search', 'profile']);
  assert.deepEqual([mine, messages, search, profile].map(node => node.textContent), ['Моё', 'Сообщения', 'Поиск', 'Профиль']);
  assert.equal(mine.getAttribute('aria-current'), 'page'); assert.equal(messages.getAttribute('aria-current'), 'false');
  messages.emit('click'); assert.deepEqual(selected, []); assert.equal(f.app.noteActionPending, true);
  dirty = false; gate.resolve(); await turn(); assert.deepEqual(selected, ['messages']);
  f.app.view = 'messages'; f.Controller.prototype.renderNavigation.call(f.app);
  assert.equal(f.app.fieldNavigation.children[1].getAttribute('aria-current'), 'page');
  f.app.appStage = {}; f.Controller.prototype.renderNavigation.call(f.app);
  assert.equal(f.app.fieldNavigation.hidden, false, 'stage visibility belongs to its mounted surface, so a late close cannot leave field navigation hidden');
  f.app.appStage = null;
  f.app.group = { communityId: 'visible-group' }; f.Controller.prototype.renderNavigation.call(f.app);
  assert.equal(f.app.fieldNavigation.hidden, false, 'global destinations stay available inside a community');
  f.app.group = null; f.app.view = 'notes'; f.Controller.prototype.renderNavigation.call(f.app);
  assert.equal(f.app.fieldNavigation.hidden, false);
  assert.equal(f.app.fieldNavigation.dataset.fieldReturn, 'true');
  assert.equal(f.app.fieldNavigation.children.length, 4);
});

test('Security Human signins action mounts the canonical editor with its exact account/device and closes the former menu', async () => {
  const f=fixture(),mounts=[];let disposed=0;
  f.ports['./human-signins']={mountHumanSignins(options){mounts.push(options);return{dispose(){disposed++;}};}};
  f.app.options.localAccount=async()=>({accountId:'account-A',deviceId:'device-A',label:'Local A'});
  f.app.dialog=function(_title,onClose){const element=new ElementPort(),body=new ElementPort();const dialog={element,body,close(){element.open=false;onClose?.({interrupted:false});}};f.dialogs.push(dialog);this.dialogs.add(dialog);return dialog;};
  f.app.openProfileMenu();const menu=f.dialogs.at(-1);
  menu.body.querySelectorAll('button').find(node=>node.dataset.accountSection==='security').emit('click');
  menu.body.querySelectorAll('button').find(node=>node.dataset.accountAction==='human-signins').emit('click');await turn();
  assert.equal(menu.element.open,false);assert.equal(mounts.length,1);
  assert.equal(mounts[0].accountId,'account-A');assert.equal(mounts[0].deviceId,'device-A');assert.equal(mounts[0].api,f.app.api);assert.equal(mounts[0].isCurrent(),true);
  f.app.accountGeneration++;assert.equal(mounts[0].isCurrent(),false);
  f.dialogs.at(-1).close();assert.equal(disposed,1);
});

test('a delayed local identity cannot mount Human signins for a new or returned account/device', async () => {
  const f=fixture(),gate=deferred(),mounts=[];
  f.ports['./human-signins']={mountHumanSignins(options){mounts.push(options);return{dispose(){}};}};
  f.app.options.localAccount=()=>gate.promise;f.app.openHumanSignins();
  f.app.deskAccount='account-B';f.app.accountGeneration++;f.app.deskAccount='account-A';f.app.accountGeneration++;
  gate.resolve({accountId:'account-A',deviceId:'device-A',label:'A'});await turn();assert.equal(mounts.length,0);
});

test('connection entry calls the host hub once and ignores failures from a departed account', async () => {
  const f = fixture(), gate = deferred(); let count = 0;
  f.app.options.openConnections = () => { count++; return gate.promise; };
  const pending = f.app.openConnections(); assert.equal(count, 1);
  f.app.accountGeneration++; gate.reject(failure('expired_connection')); await pending;
  assert.equal(f.app.lastToast, undefined);
  f.app.options.openConnections = () => { throw failure('connection_unavailable'); };
  await f.app.openConnections(); assert.equal(f.app.lastToast, 'connection_unavailable');
});

test('community sharing emits a canonical root link from PWA and tool entries without ambient query state', async () => {
  const f = fixture(), links = [];
  f.view.navigator.clipboard = { async writeText(value) { links.push(value); } };
  for (const href of ['https://shell.example/?pwa=1#world', 'https://shell.example/tool/?unrelated=fixture#mine']) {
    f.location.href = href; f.app.shareCommunity({ communityId: 'group-12345' }); await turn();
  }
  assert.deepEqual(links, ['https://shell.example/#community/group-12345/about', 'https://shell.example/#community/group-12345/about']);
});

test('contact open uses the current account and exact accepted relationship, then selects a genuine direct thread', async () => {
  const f=fixture(), calls=[], navigation=[];f.app.profile.directMessagesVersion=1;
  f.app.api={async request(op,args){calls.push({op,args});return {current:{threadId:'direct_fixture',relationshipId:'rel-fixture',peer:{accountId:'peer-fixture',label:null}}};}};
  f.app.navigate=(view,id)=>navigation.push({view,id});
  await f.app.openDirectContact({peerAccountId:'peer-fixture',relationshipId:'rel-fixture',label:'Друг'});
  assert.equal(calls[0].op,'world.direct.open');assert.equal(calls[0].args.expectedAccountId,'account-A');assert.equal(calls[0].args.expectedRelationshipId,'rel-fixture');
  assert.deepEqual(navigation,[{view:'messages',id:'direct_fixture'}]);assert.equal(f.app.directLabels.get('peer-fixture'),'Друг');
  assert.equal(f.app.directThreads[0].peer.label,null,'contact presentation does not manufacture a World profile');
});

test('a late direct-open response cannot populate or navigate another account', async () => {
  const f=fixture(), gate=deferred(), navigation=[];f.app.profile.directMessagesVersion=1;
  f.app.api={request:()=>gate.promise};f.app.navigate=(...args)=>navigation.push(args);
  const pending=f.app.openDirectContact({peerAccountId:'peer-fixture',relationshipId:'rel-fixture',label:'Друг'});
  f.app.transitionAccount('account-B');gate.resolve({current:{threadId:'direct_fixture',relationshipId:'rel-fixture',peer:{accountId:'peer-fixture'}}});await pending;
  assert.deepEqual(navigation,[]);assert.equal(f.app.directThreads.length,0);assert.equal(f.app.directLabels.size,0);
});

function notificationFixture() {
  const f = fixture(), calls = [], navigations = [];
  f.view.document.querySelector = () => null;
  f.app.chatDrafts = { hasVolatile: () => false };
  f.app.navigateReady = (view, id) => navigations.push({ view, id });
  const room = { communityId: 'room_fixture', revision: 1, membership: { state: 'active', muted: true } };
  const direct = { threadId: 'direct_fixture', relationshipId: 'rel_fixture', peer: { accountId: 'peer_fixture' } };
  f.app.api = { async request(op, args) { calls.push({ op, args: JSON.parse(JSON.stringify(args)) }); return op === 'world.community.get' ? { community: room } : { thread: direct }; } };
  const request = { async assertCurrent() {}, signal: new AbortController().signal };
  return { ...f, calls, navigations, room, direct, request };
}

test('notification click rechecks the exact current conversation and opens a muted room or a genuine DM', async () => {
  const f = notificationFixture();
  await f.app.openNotification({ kind: 'room', communityId: f.room.communityId }, f.request);
  await f.app.openNotification({ kind: 'direct', threadId: f.direct.threadId, peerAccountId: f.direct.peer.accountId, relationshipId: f.direct.relationshipId }, f.request);
  assert.deepEqual(f.navigations, [{ view: 'messages', id: f.room.communityId }, { view: 'messages', id: f.direct.threadId }]);
  assert.deepEqual(f.calls.map(row => row.op), ['world.community.get', 'world.direct.get']);
  assert.deepEqual(f.calls[1].args, { expectedAccountId: 'account-A', threadId: f.direct.threadId, expectedRelationshipId: f.direct.relationshipId });
  assert.equal(f.app.directThreads[0].peer.accountId, 'peer_fixture');
});

test('notification navigation refuses a mismatched thread, relationship, peer or inactive room', async () => {
  for (const change of [thread => { thread.threadId = 'direct_other'; }, thread => { thread.relationshipId = 'rel_other'; }, thread => { thread.peer.accountId = 'peer_other'; }]) {
    const f = notificationFixture(), target = { kind: 'direct', threadId: 'direct_fixture', peerAccountId: 'peer_fixture', relationshipId: 'rel_fixture' };
    change(f.direct); await assert.rejects(f.app.openNotification(target, f.request), /push_notification_unavailable/u);
    assert.deepEqual(f.navigations, []); assert.deepEqual(f.app.directThreads, []);
  }
  const f = notificationFixture(); f.room.membership.state = 'left';
  await assert.rejects(f.app.openNotification({ kind: 'room', communityId: f.room.communityId }, f.request), /push_notification_unavailable/u);
  assert.deepEqual(f.navigations, []);
});

test('late notification lookup cannot override a new account, screen or aborted click', async () => {
  for (const change of [f => { f.app.accountGeneration++; }, f => { f.app.screenSequence++; }, f => { f.aborter.abort(); }]) {
    const f = notificationFixture(), gate = deferred(); f.aborter = new AbortController(); f.request.signal = f.aborter.signal;
    f.app.api = { request: () => gate.promise };
    const pending = f.app.openNotification({ kind: 'room', communityId: f.room.communityId }, f.request);
    await turn(); change(f); gate.resolve({ community: f.room });
    await assert.rejects(pending, /push_actor_denied/u); assert.deepEqual(f.navigations, []);
  }
});

test('notification click waits for durable work and preserves editing that starts during its lookup', async () => {
  const f = notificationFixture(), flush = deferred(); let dirty = true;
  f.app.notesHandle = { flush: () => flush.promise, hasUnsavedChanges: () => dirty };
  const pending = f.app.openNotification({ kind: 'room', communityId: f.room.communityId }, f.request);
  await turn(); assert.equal(f.calls.length, 0); dirty = false; flush.resolve(); await pending;
  assert.equal(f.navigations.length, 1);
  const late = notificationFixture(), lookup = deferred(); let edited = false;
  late.app.chatDrafts = { hasVolatile: () => edited }; late.app.api = { request: () => lookup.promise };
  const result = late.app.openNotification({ kind: 'room', communityId: late.room.communityId }, late.request);
  await turn(); edited = true; lookup.resolve({ community: late.room });
  await assert.rejects(result, /Сохраните/u); assert.deepEqual(late.navigations, []); assert.equal(edited, true);
});

test('the actual root capture leaves notification and Help controls operable while other work is dirty', () => {
  const f = fixture();
  let guarded = 0, prevented = 0;
  f.app.screenHasUnsavedChanges = () => true; f.app.afterNoteSaved = () => guarded++;
  const capture = f.rootClick;
  // A small CSS-selector DOM port, not a copy of the capture handler's policy.
  const surface = (className, parent = null) => {
    const node = new ElementPort(); node.tagName = parent ? 'button' : 'div'; node.className = className; node.parent = parent;
    node.closest = selector => {
      for (let value = node; value; value = value.parent) if (selector.split(',').some(part => part.trim().startsWith('.')
        ? value.className.split(/\s+/u).includes(part.trim().slice(1)) : value.tagName === part.trim())) return value;
      return null;
    }; return node;
  };
  const toast = surface('sw-toast has-action'), retry = surface('sw-button', toast), close = surface('sw-button', toast);
  const event = target => ({ target, preventDefault() { prevented++; }, stopImmediatePropagation() {} });
  capture(event(retry)); capture(event(close)); assert.equal(guarded, 0); assert.equal(prevented, 0);
  const help = surface('sw-help-dialog'), helpClose = surface('sw-icon-button', surface('sw-dialog-header', help));
  capture(event(helpClose)); capture(event(surface('sw-button', help))); assert.equal(guarded, 0); assert.equal(prevented, 0);
  capture(event(surface('sw-button', surface('sw-dialog')))); assert.equal(guarded, 1); assert.equal(prevented, 1);
  capture(event(surface('sw-button', surface('ordinary-panel')))); assert.equal(guarded, 2); assert.equal(prevented, 2);
});

test('existing group chat mounts the common renderer with actual group authority and the original draft namespace', () => {
  const f=fixture(), captured=[];
  f.app.mountConversation=(parent,target,presentation)=>{captured.push({parent,target,presentation});return ()=>true;};
  const group={communityId:'group-real',name:'Комната',memberCount:2,membership:{state:'active',pinned:false,muted:false},permissions:{canWrite:true,canModerate:true}};
  const parent=new ElementPort();f.app.mountChat(parent,group,true);
  assert.equal(captured[0].target.kind,'community');assert.equal(captured[0].target.draftKey,group.communityId);
  assert.equal(captured[0].target.record,group);assert.equal(captured[0].target.canModerate,true);assert.equal(captured[0].presentation.showHeader,true);
  assert.equal(captured[0].presentation.extraActions().length,2);
});

test('a closed chat cannot restore membership through a late metadata response on the same screen', async () => {
  const f=fixture(), gate=deferred();let target, mounted=true;
  f.app.mountConversation=(_parent,value)=>{target=value;return ()=>mounted;};
  const group={communityId:'group-real',name:'Комната',memberCount:2,membership:{state:'active',pinned:false,muted:false},permissions:{canWrite:true,canModerate:true}};
  f.app.api={request:()=>gate.promise};f.app.communities=[group];f.app.mountChat(new ElementPort(),group,true);
  const pending=target.refresh();mounted=false;gate.resolve({community:{...group,revision:99}});
  await assert.rejects(pending,{code:'conversation_changed'});assert.equal(f.app.communities[0],group);
});

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

test('a newly mounted field completes its initial load once; later shell refreshes still refresh that field', async () => {
  const f=fixture(),initial=deferred();let refreshes=0,settled=false;
  const field={ready:initial.promise,async refresh(){refreshes++;},dispose(){}};
  f.app.renderCurrent=()=>{f.app.unifiedField=field;};
  const opening=f.app.refresh().then(()=>{settled=true;});await turn();
  assert.equal(f.app.unifiedField,field);assert.equal(refreshes,0);assert.equal(settled,false);
  initial.resolve();await opening;assert.equal(refreshes,0,'initialization already resolves the field metadata');
  await f.app.refresh();assert.equal(refreshes,1,'an existing field still receives an explicit refresh');
});

test('a late successful health check does not supersede the initial held profile read and still recovers an idle unverified shell',async()=>{
  const f=fixture(),pending=deferred(),original=f.app.api.request;let profileReads=0;
  f.app.profile=null;f.app.connection='checking';f.app.renderPwaBanner=()=>{};f.app.syncFieldOutbox=()=>{};
  f.app.renderCurrent=()=>{};f.app.api.request=(op,args)=>{
    if(op==='world.profile.get'&&++profileReads===1)return pending.promise;
    return original(op,args);
  };
  const opening=f.app.refresh();await turn();const sequence=f.app.requestSequence;
  f.connectionChanged({connection:'online'});await turn();
  assert.equal(profileReads,1);assert.equal(f.app.requestSequence,sequence);assert.equal(f.app.refreshInFlight,sequence);
  pending.resolve({profile:{profileId:'account-A',displayName:'verified-A'}});await opening;
  assert.equal(f.app.refreshInFlight,0);assert.equal(f.app.profile.displayName,'verified-A');
  f.app.profile=null;f.app.connection='unreachable';f.connectionChanged({connection:'online'});await turn();
  assert.equal(profileReads,2);assert.equal(f.app.refreshInFlight,0);assert.equal(f.app.profile.profileId,'account-A');
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
    const support = f.app.main.children[0].children.find(node => node.tagName === 'a');
    assert.ok(support, 'Core identity error must still offer the public support form');
    assert.equal(support.href, 'https://feedback.4-2.xn--p1ai/p/fp_e10dd963e039c476c9139a9a2a7d4749');
    assert.equal(support.target, '_blank'); assert.equal(support.rel, 'noopener noreferrer');
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
  assert.deepEqual(calls.map(({ op, args }) => [op, args.path]), [['apps.entry.get', '/']], 'a cold discussion resolves the exact entry without a runtime admission');
  assert.equal(f.app.appStage.entry().path, '/'); assert.equal(appLaunch.parseAppLaunchRoute(f.location.hash).target.path, '/');
  const failures = calls.length; await turn(); assert.equal(calls.length, failures, 'no automatic default retry of a refused deep link');
  calls.length = 0; await f.app.openApplication(cached);
  assert.equal(calls.length, 1); assert.equal(Object.hasOwn(calls[0].args, 'path'), false);
  assert.equal(f.app.appStage.entry().path, '/embed'); assert.ok(f.app.main.querySelector('iframe'));
  f.app.cleanScreen();
});
function bootHint(f, frame) {
  frame.emit('load'); const sent = frame.contentWindow.sent.findLast(value => value.data?.schema === 'soty.app-boot-watch.v1'); assert.ok(sent);
  assert.equal(sent.origin, 'https://runtime.example');
  assert.equal(sent.data.appId, stageAppId); assert.match(sent.data.nonce, /^[A-Za-z0-9_-]{43}$/u);
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

async function staleSourceStage({ lookup, open, discussion = false } = {}) {
  const f = fixture({ hash: '#world' }), account = f.app.deskAccount, generation = f.app.accountGeneration;
  const intent = appLaunch.parseAppLaunchRoute(appLaunch.formatAppLaunchRoute({ appId: stageAppId, domainId: stageDomainId, path: '/embed' },
    undefined, discussion ? { panel: 'discussion' } : undefined));
  const stale = { appId: stageAppId, domainId: stageDomainId, origin: 'https://runtime.example', path: '/embed' };
  const requests = [], opened = []; let launches = 0;
  f.app.api = { async request(op, args) {
    requests.push({ op, args: structuredClone(args) }); assert.equal(op, 'apps.entry.get');
    return args.current ? lookup() : { entry: stale };
  } };
  f.app.appStage = f.mountStage(f.app.main, { api: f.app.api, accountId: account,
    app: { appId: stageAppId, name: 'Moved Source', grants: { accountIds: [account], communityIds: [] } }, intent,
    presentation: 'widget', instanceId: 'widget-11111111-1111-4111-8111-111111111111',
    request: async () => { launches++; throw failure('scoped_embed_source_changed'); },
    isCurrent: () => !f.app.destroyed && f.app.deskAccount === account && f.app.accountGeneration === generation,
    onOpenCurrentEntry: async entry => { opened.push(entry); await open?.(entry); },
    onNavigate() {}, onBack() {}, async onAccount() {}, onSettings() {},
  });
  await f.app.appStage.ready;
  if (discussion) {
    assert.equal(launches, 0, 'cold discussion does not issue runtime admission');
    f.app.main.querySelectorAll('button').find(node => node.dataset.stageControl === 'application').emit('click'); await turn();
    f.app.main.querySelectorAll('button').find(node => node.dataset.stageControl === 'discussion').emit('click'); await turn();
  }
  return { ...f, currentRequests: requests, opened, stale, launches: () => launches };
}

test('a stale Source address offers one explicit current-entry lookup without rebinding its original launcher', async t => {
  const pending = deferred(), fresh = { appId: stageAppId, domainId: `dom_${'f'.repeat(32)}`, origin: 'https://current.example', path: '/embed' };
  const f = await staleSourceStage({ lookup: () => pending.promise }); t.after(() => f.app.cleanScreen());
  const retry = f.app.main.querySelector('.sa-current-entry-retry');
  assert.equal(retry.hidden, false); assert.equal(f.app.main.querySelector('.sa-recovery-retry').hidden, true);
  assert.match(f.app.main.querySelector('.sa-runtime').children[0].textContent, /Адрес приложения изменился/u);
  assert.equal(f.currentRequests.length, 1, 'failed launch only resolved its original exact entry');
  retry.emit('click'); retry.emit('click'); await turn();
  assert.deepEqual(f.currentRequests.at(-1), { op: 'apps.entry.get', args: { appId: stageAppId, current: true, expectedAccountId: 'account-A' } });
  assert.equal(f.currentRequests.length, 2); assert.equal(retry.disabled, true);
  pending.resolve({ entry: fresh }); await turn();
  assert.deepEqual(f.opened, [fresh]); assert.equal(retry.disabled, false);
  assert.deepEqual(f.app.appStage.entry(), f.stale, 'the captured old address was not silently changed');
  assert.equal(f.launches(), 1); assert.equal(f.app.main.querySelector('iframe'), null);
});

for (const transition of ['account ABA', 'grant removal']) test(`a late current-entry reply cannot navigate after ${transition}`, async t => {
  const pending = deferred(), f = await staleSourceStage({ lookup: () => pending.promise }); t.after(() => f.app.cleanScreen());
  f.app.main.querySelector('.sa-current-entry-retry').emit('click'); await turn();
  if (transition === 'account ABA') { f.app.transitionAccount('account-B'); f.app.transitionAccount('account-A'); }
  else f.app.appStage.updateApp({ appId: stageAppId, name: 'Moved Source', grants: { accountIds: [], communityIds: [] } });
  pending.resolve({ entry: { appId: stageAppId, domainId: `dom_${'f'.repeat(32)}`, origin: 'https://current.example', path: '/embed' } }); await turn();
  assert.deepEqual(f.opened, []); assert.equal(f.launches(), 1);
});

test('a mismatched current-entry DTO cannot substitute another app', async t => {
  const f = await staleSourceStage({ lookup: async () => ({ entry: { appId: `app-${'f'.repeat(32)}`,
    domainId: `dom_${'f'.repeat(32)}`, origin: 'https://current.example', path: '/embed' } }) }); t.after(() => f.app.cleanScreen());
  f.app.main.querySelector('.sa-current-entry-retry').emit('click'); await turn();
  assert.deepEqual(f.opened, []); assert.equal(f.launches(), 1);
  assert.match(f.app.main.querySelector('.sa-message').textContent, /безопасно открыть/u);
});

test('current-entry navigation preserves an unsaved discussion and makes no lookup until it can leave', async t => {
  const f = await staleSourceStage({ discussion: true, lookup: async () => { throw Error('must_not_request'); } }); t.after(() => f.app.cleanScreen());
  f.discussionCalls.unsaved = true;
  f.app.main.querySelector('.sa-current-entry-retry').emit('click'); await turn();
  assert.equal(f.currentRequests.length, 1); assert.deepEqual(f.opened, []);
  assert.match(f.app.main.querySelector('.sa-message').textContent, /Сохраните незавершённое действие/u);
  assert.equal(f.app.main.querySelector('.sa-current-entry-retry').disabled, false);
});

for (const profile of ['legacy', 'scoped']) test(`${profile} widget no-load has a bounded UNKNOWN notice and manual retry without another launch or API request`, async t => {
  let calls = 0;
  const f = await widgetStage(async () => { calls++; return { ...stageResponse('/'), ...(profile === 'scoped' ? {
    runtimeProfile: 'soty.selected-human-embed.v2', scopedCloseHandle: 'h'.repeat(43),
    scopedSource: { id: 'soty.standard-workspace', version: 2, digest: 'a'.repeat(64) },
  } : {}) }; }); t.after(() => f.app.cleanScreen());
  const runtime = f.app.main.querySelector('.sa-runtime'), message = f.app.main.querySelector('.sa-message');
  const retry = f.app.main.querySelector('.sa-recovery-retry'), deadline = [...f.timers.values()].find(value => value.delay === 12000);
  const separate = f.app.main.querySelector('.sa-external-recovery');
  assert.ok(deadline); assert.match(message.textContent, /Открываем окно/u); assert.equal(runtime.getAttribute('aria-busy'), 'true');
  assert.equal(separate.hidden, true, 'the separate-window fallback waits for an actual loading failure');
  assert.equal(retry.hidden, false); assert.equal(retry.disabled, false);
  deadline.fn(); await turn();
  assert.match(message.textContent, /не подтверждена/u); assert.equal(runtime.getAttribute('aria-busy'), 'false');
  assert.equal(retry.hidden, false); assert.equal(calls, 1); assert.deepEqual(f.requests, []); assert.equal(f.timers.size, 0);
  assert.equal(separate.hidden, false, 'an unconfirmed frame exposes the existing explicit external-launch action');
  f.app.main.querySelector('iframe').emit('load'); await turn();
  assert.equal(separate.hidden, false, 'a late load cannot attest Native paint or clear an unconfirmed notice');
  if (profile === 'legacy') assert.equal(message.hidden, true, 'late Native document load stays neutral, without an authenticated-paint assertion');
  else assert.match(message.textContent, /Окно загружено, но запуск приложения не подтверждён/u);
  assert.equal(calls, 1); assert.deepEqual(f.requests.map(value => value.op), profile === 'scoped' ? ['apps.scoped.context'] : []); assert.equal(f.timers.size, 0);
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
  if (profile === 'legacy') {
    assert.equal(message.hidden, true, 'a loaded legacy document has a neutral transport state, never an authenticated-paint assertion');
    assert.equal(f.app.main.querySelector('.sa-runtime').getAttribute('aria-busy'), 'false');
    const external = f.app.main.querySelector('.sa-external-recovery');
    assert.equal(external.hidden, false); assert.equal(external.disabled, false);
    assert.equal(f.app.main.querySelector('iframe'), frame); assert.equal(calls, 1);
    assert.deepEqual(f.requests, []); assert.equal(f.timers.size, 0);
    return;
  }
  assert.equal(message.hidden, false); assert.match(message.textContent, /Открываем окно/u);
  assert.equal(f.app.main.querySelector('.sa-runtime').getAttribute('aria-busy'), 'true');
  assert.equal(f.app.main.querySelector('.sa-recovery-retry').hidden, false);
  assert.equal(calls, 1);
  assert.deepEqual(f.requests.map(value => value.op), profile === 'scoped' ? ['apps.scoped.context'] : []);
  const deadline = [...f.timers.values()].find(value => value.delay === 12000); assert.ok(deadline);
  deadline.fn(); await turn();
  assert.match(message.textContent, /запуск приложения не подтверждён/u);
  assert.equal(f.app.main.querySelector('.sa-runtime').getAttribute('aria-busy'), 'false');
  assert.equal(f.app.main.querySelector('iframe'), frame); assert.equal(calls, 1); assert.equal(f.timers.size, 0);
  assert.deepEqual(f.requests.map(value => value.op), profile === 'scoped' ? ['apps.scoped.context'] : []);
});

test('widget transport error permits one explicit retry and ignores queued events from the replaced frame', async t => {
  let calls = 0; const pending = deferred();
  const f = await widgetStage(async () => ++calls === 1 ? stageResponse('/') : pending.promise); t.after(() => f.app.cleanScreen());
  const first = f.app.main.querySelector('iframe'), oldEvents = [...first.handlers.get('load'), ...first.handlers.get('error')];
  first.emit('error');
  const retry = f.app.main.querySelector('.sa-recovery-retry');
  assert.match(f.app.main.querySelector('.sa-message').textContent, /Не удалось подтвердить запуск приложения/u);
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
  const actions = f.app.main.querySelector('.sa-stage-runtime-notice'), retry = actions.querySelector('.sa-recovery-retry');
  assert.equal(retry.parent, actions); assert.equal(retry.textContent, 'Открыть приложение заново');
  assert.notEqual(retry, f.app.main.querySelector('.sa-external-recovery'));
  assert.equal(retry.hidden, false);
  assert.match(f.app.main.querySelector('.sa-message').textContent, /Откройте приложение заново/u);
  f.app.cleanScreen(); assert.equal(f.timers.size, 0);
});

test('actual stage refuses a display-watch nonce as boot recovery while the separate recovery nonce still works', async t => {
  let calls = 0;
  const f = await recoveryStage(async () => { calls++; return { ...stageResponse('/'), launchBinding: stageBinding }; }); t.after(() => f.app.cleanScreen());
  const frame = f.app.main.querySelector('iframe'); frame.emit('load');
  const display = frame.contentWindow.sent.findLast(value => value.data.schema === 'soty.runtime-display-watch.v1');
  const recovery = frame.contentWindow.sent.findLast(value => value.data.schema === 'soty.app-boot-watch.v1');
  assert.ok(display); assert.ok(recovery); assert.match(display.data.nonce, /^[a-f0-9]{32}$/u); assert.match(recovery.data.nonce, /^[A-Za-z0-9_-]{43}$/u);
  const emit = nonce => f.view.dispatchEvent(Object.assign(new Event('message'), { source: frame.contentWindow, origin: 'https://runtime.example', ports: [],
    data: { schema: 'soty.app-boot-failure.v1', appId: stageAppId, nonce, error: 'app_session_check_failed' } }));
  emit(display.data.nonce); await turn();
  assert.equal(calls, 1); assert.equal(f.app.main.querySelector('iframe'), frame);
  emit(recovery.data.nonce); await turn();
  assert.equal(calls, 2); assert.notEqual(f.app.main.querySelector('iframe'), frame);
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
    const actions = f.app.main.querySelector('.sa-stage-runtime-notice'), retry = actions.querySelector('.sa-recovery-retry');
    assert.equal(retry.parent, actions); assert.equal(retry.textContent, 'Открыть приложение заново');
    assert.notEqual(retry, f.app.main.querySelector('.sa-external-recovery'));
    assert.equal(retry.hidden, false);
    assert.equal(f.app.main.querySelector('.sa-message').hidden, false); f.app.cleanScreen();
  }
});

test('legacy launch response mounts its runtime without requiring new metadata or a recovery message channel', async () => {
  let calls = 0; const f = await recoveryStage(async () => { calls++; return stageResponse('/'); });
  const original = f.app.main.querySelector('iframe'); original.emit('load'); await turn();
  assert.equal(original.contentWindow.sent.some(value => value.data?.schema === 'soty.app-boot-watch.v1'), false);
  assert.ok(original.contentWindow.sent.some(value => value.data?.schema === 'soty.runtime-display-watch.v1'));
  assert.equal(calls, 1);
  assert.deepEqual(f.requests, [], 'legacy launch does not acquire a new Source-session dependency');
  assert.equal(f.timers.size, 0, 'neutral transport load is not a deadline or Native readiness attestation');
  assert.equal(f.app.main.querySelector('iframe'), original); assert.equal(calls, 1);
  assert.equal(f.app.main.querySelector('.sa-message').hidden, true);
  assert.equal(f.app.main.querySelector('.sa-runtime').getAttribute('aria-busy'), 'false');
  const external = f.app.main.querySelector('.sa-external-recovery');
  assert.equal(external.hidden, false); assert.equal(external.disabled, false);
  assert.equal(external.parent, f.app.main.querySelector('.sa-stage-runtime-notice'), 'the existing action is outside the CSS-hidden actions group');
  assert.equal(f.timers.size, 0); f.app.cleanScreen();
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
  assert.equal(calls, 0); assert.equal(f.app.main.querySelector('iframe'), null, 'ordinary cold discussion also defers runtime');
  f.app.main.querySelectorAll('button').find(node => node.dataset.stageControl === 'application').emit('click'); await turn();
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

// Append to src/world/app-account-lifecycle.test.mjs. Uses that file's actual
// World/Application stage fixture; no copied recovery callback implementation.
const currentReviewEntry = { appId: stageAppId, domainId: `dom_${'f'.repeat(32)}`, origin: 'https://current.example', path: '/embed' };
const oldReviewEntry = { appId: stageAppId, domainId: stageDomainId, origin: 'https://runtime.example', path: '/embed' };
const currentReviewResponse = () => ({ entry: currentReviewEntry,
  url: `${currentReviewEntry.origin}/_soty/boot?${new URLSearchParams({ path: currentReviewEntry.path })}#${'q'.repeat(43)}` });

async function currentReviewMain({ discussion = false } = {}) {
  const f = fixture({ hash: '#world' }), calls = [], lookups = [];
  f.app.options.openApp = async (_app, args) => { calls.push(structuredClone(args));
    if (args.domainId === stageDomainId) throw failure('scoped_embed_source_changed');
    return currentReviewResponse();
  };
  f.app.api = { async request(op, args) { assert.equal(op, 'apps.entry.get'); lookups.push(structuredClone(args)); return { entry: args.current ? currentReviewEntry : oldReviewEntry }; } };
  await f.app.openApplication({ appId: stageAppId, name: 'Moved application' }, appLaunch.parseAppLaunchRoute(appLaunch.formatAppLaunchRoute({ appId: stageAppId, domainId: stageDomainId, path: '/embed' }, undefined, discussion ? { panel: 'discussion' } : undefined)));
  if (discussion) {
    assert.equal(calls.length, 0, 'cold discussion has no runtime admission');
    f.app.main.querySelectorAll('button').find(node => node.dataset.stageControl === 'application').emit('click'); await turn();
    f.app.main.querySelectorAll('button').find(node => node.dataset.stageControl === 'discussion').emit('click'); await turn();
  }
  return { ...f, calls, lookups };
}

test('actual main UI current-entry button creates a new exact stage, retaining the old launcher target and excluding origin from its route', async t => {
  const f = await currentReviewMain({ discussion: true }); t.after(() => f.app.cleanScreen()); const oldStage = f.app.appStage;
  const retry = f.app.main.querySelector('.sa-current-entry-retry'); assert.equal(retry.hidden, false);
  retry.emit('click'); await turn(); await turn();
  assert.notEqual(f.app.appStage, oldStage); assert.deepEqual(f.app.appStage.entry(), currentReviewEntry);
  assert.equal(f.calls.length, 2); assert.equal(f.calls[0].domainId, stageDomainId); assert.equal(f.calls[1].domainId, currentReviewEntry.domainId);
  assert.equal(f.calls[1].path, '/embed'); assert.equal(f.calls[1].expectedAccountId, 'account-A');
  assert.equal(f.location.hash, '#' + appLaunch.formatAppLaunchRoute({ appId: stageAppId, domainId: currentReviewEntry.domainId, path: '/embed' }));
  assert.equal(f.lookups.filter(args => args.current).length, 1); assert.ok(f.app.main.querySelector('iframe'));
});

test('actual main UI recovery cannot discard an unsaved discussion', async t => {
  const f = await currentReviewMain({ discussion: true }); t.after(() => f.app.cleanScreen()); const oldStage = f.app.appStage;
  f.discussionCalls.unsaved = true; f.app.main.querySelector('.sa-current-entry-retry').emit('click'); await turn();
  assert.equal(f.app.appStage, oldStage); assert.equal(f.calls.length, 1); assert.equal(f.lookups.filter(args => args.current).length, 0);
});

test('actual main UI current-entry lookup cannot reopen a stage after account ABA', async t => {
  const f = await currentReviewMain(), gate = deferred(); t.after(() => f.app.cleanScreen());
  f.app.api.request = async (_op, args) => args.current ? gate.promise : { entry: oldReviewEntry };
  f.app.main.querySelector('.sa-current-entry-retry').emit('click'); await turn();
  f.app.transitionAccount('account-B'); f.app.transitionAccount('account-A'); gate.resolve({ entry: currentReviewEntry }); await turn();
  assert.equal(f.app.appStage, null); assert.equal(f.calls.length, 1);
});

// The World constructor supplies the widget's callback. Extract its actual
// arrow initializer, then execute it with actual MicroWidgets and AppStage.
const currentReviewAst = ts.createSourceFile('app.ts', source, ts.ScriptTarget.Latest, true); let currentReviewWidgetMount;
function currentReviewVisit(node) {
  if (ts.isCallExpression(node) && node.expression.getText(currentReviewAst) === 'mountMicroWidgets')
    currentReviewWidgetMount = node.arguments[1].properties.find(property => property.name?.getText(currentReviewAst) === 'mount')?.initializer.getText(currentReviewAst);
  ts.forEachChild(node, currentReviewVisit);
}
currentReviewVisit(currentReviewAst); assert.ok(currentReviewWidgetMount);

async function currentReviewWidgets() {
  const f = fixture({ hash: '#world' }), data = new Map(), stages = [], calls = [], lookups = [];
  const workspace = await import(new URL('./workspace-widgets.mjs', import.meta.url));
  const floating = await import(new URL('./floating-layout.ts', import.meta.url));
  const storage = { getItem: key => data.get(key) ?? null, setItem: (key, value) => data.set(key, value) };
  Object.assign(f.view, { localStorage: storage, innerWidth: 390, innerHeight: 844 });
  const root = f.app.root, document = root.ownerDocument;
  ElementPort.prototype.dispatchEvent = function(event) { for (const fn of [...this.handlers.get(event.type) || []]) fn(event); return !event.defaultPrevented; };
  const make = (tagName, className = '', textContent = '') => Object.assign(new ElementPort(), { ownerDocument: document, tagName, className, textContent });
  const button = (label, _icon, className, action) => { const node = make('button', className, label); if (action) node.addEventListener('click', action); return node; };
  const exported = {};
  const code = ts.transpileModule(await readFile(new URL('./micro-widgets.ts', import.meta.url), 'utf8'), { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
  vm.runInNewContext(code, { exports: exported, AbortController, Event, crypto, queueMicrotask,
    require: name => name === './dom' ? { el: make, button, iconButton: (label, icon, action) => button(label, icon, '', action) }
      : name === './workspace-widgets.mjs' ? workspace : name === './app-launch.mjs' ? appLaunch : name === './floating-layout' ? floating : {},
  });
  f.app.options.openApp = async (_app, args) => { calls.push(structuredClone(args));
    if (args.domainId === stageDomainId) throw failure('scoped_embed_source_changed');
    return currentReviewResponse();
  };
  f.app.api = { async request(op, args) { assert.equal(op, 'apps.entry.get'); lookups.push(structuredClone(args)); return { entry: args.current ? currentReviewEntry : oldReviewEntry }; } };
  const binder = {};
  vm.runInNewContext(ts.transpileModule(`exports.bind = function(mountAppStage, parseAppLaunchRoute, formatAppLaunchRoute) { return ${currentReviewWidgetMount}; };`, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText, { exports: binder });
  const mount = binder.bind.call(f.app, f.mountStage, appLaunch.parseAppLaunchRoute, appLaunch.formatAppLaunchRoute);
  const original = { id: 'widget-11111111-1111-4111-8111-111111111111', name: 'Moved app', route: appLaunch.formatAppLaunchRoute({ appId: stageAppId, domainId: stageDomainId, path: '/embed' }), visible: true, x: .81, y: .37, width: 520, height: 440 };
  workspace.editWidgetWorkspace(storage, f.app.deskAccount, state => { state.widgets.push(original); });
  f.app.widgets = exported.mountMicroWidgets(root, { mount(...args) { const stage = mount(...args); stages.push(stage); return stage; }, dialog: f.app.dialog.bind(f.app), notify: f.app.toast.bind(f.app) });
  f.app.widgets.setAccount(f.app.deskAccount); await stages[0].ready;
  return { ...f, workspace, storage, original, stages, calls, lookups };
}

test('actual widget UI current-entry button replaces exactly that stage and retains its saved id, geometry and visibility', async t => {
  const f = await currentReviewWidgets(); t.after(() => f.app.widgets.dispose()); const oldStage = f.stages[0];
  const retry = f.app.root.querySelector('.sa-current-entry-retry'); assert.equal(retry.hidden, false);
  retry.emit('click'); await turn(); await turn();
  assert.equal(f.stages.length, 2); assert.notEqual(f.stages[1], oldStage); assert.deepEqual(f.stages[1].entry(), currentReviewEntry);
  assert.deepEqual(f.workspace.readWidgetWorkspace(f.storage, 'account-A').widgets, [{ ...f.original, route: appLaunch.formatAppLaunchRoute({ appId: stageAppId, domainId: currentReviewEntry.domainId, path: '/embed' }) }]);
  assert.equal(f.calls.length, 2); assert.equal(f.calls[0].domainId, stageDomainId); assert.equal(f.calls[1].domainId, currentReviewEntry.domainId);
  assert.equal(f.calls[0].instanceId, f.original.id); assert.equal(f.calls[1].instanceId, f.original.id);
  assert.equal(f.location.hash, '#world', 'widget recovery leaves the main human screen in place');
});

test('actual widget UI lookup from an older account generation cannot replace its restored widget after ABA', async t => {
  const f = await currentReviewWidgets(), gate = deferred(); t.after(() => f.app.widgets.dispose());
  f.app.api.request = async (_op, args) => args.current ? gate.promise : { entry: oldReviewEntry };
  f.app.root.querySelector('.sa-current-entry-retry').emit('click'); await turn();
  f.app.widgets.setAccount('account-B'); f.app.widgets.setAccount('account-A'); await f.stages.at(-1).ready;
  gate.resolve({ entry: currentReviewEntry }); await turn();
  assert.equal(f.stages.length, 2); assert.equal(f.calls.length, 2);
  assert.deepEqual(f.workspace.readWidgetWorkspace(f.storage, 'account-A').widgets, [f.original]);
});

function currentReviewFind(node, predicate) {
  if (predicate(node)) return node;
  for (const child of node.children || []) { const found = currentReviewFind(child, predicate); if (found) return found; }
  return null;
}

test('temporary external-recovery button performs the existing gesture popup launch for the exact widget entry without replacing its frame', async t => {
  const admissions = [], f = await widgetStage(async args => { admissions.push(structuredClone(args)); return stageResponse('/'); });
  t.after(() => f.app.cleanScreen()); ElementPort.prototype.click = function() { this.emit('click'); };
  const frame = f.app.main.querySelector('iframe'), fallback = currentReviewFind(f.app.main, node => node.dataset?.stageControl === 'external-recovery');
  let opens = 0, navigated = null;
  const popup = { closed: false, opener: {}, location: { replace(url) { navigated = url; } }, close() { this.closed = true; } };
  f.view.open = () => { opens++; return popup; };
  const deadline = [...f.timers.values()].find(timer => timer.delay === 12000); deadline.fn(); assert.equal(fallback.hidden, false);
  fallback.emit('click'); assert.equal(opens, 1, 'the popup is created synchronously in the original click');
  await turn(); await turn();
  assert.equal(admissions.length, 2); assert.deepEqual(admissions[1], admissions[0]); assert.ok(navigated?.startsWith('https://runtime.example/_soty/boot?'));
  assert.equal(popup.opener, null); assert.equal(popup.closed, false); assert.equal(f.app.main.querySelector('iframe'), frame); assert.equal(f.location.hash, '#world');
});

test('temporary external recovery closes its unfinished popup on account change without navigating it with a late private URL', async t => {
  const gate = deferred(); let launches = 0;
  const f = await widgetStage(async () => ++launches === 1 ? stageResponse('/') : gate.promise); t.after(() => f.app.cleanScreen());
  ElementPort.prototype.click = function() { this.emit('click'); }; let navigated = null;
  const popup = { closed: false, opener: {}, location: { replace(url) { navigated = url; } }, close() { this.closed = true; } }; f.view.open = () => popup;
  [...f.timers.values()].find(timer => timer.delay === 12000).fn();
  currentReviewFind(f.app.main, node => node.dataset?.stageControl === 'external-recovery').emit('click'); await turn();
  f.app.transitionAccount('account-B'); gate.resolve(stageResponse('/')); await turn();
  assert.equal(popup.closed, true); assert.equal(navigated, null); assert.equal(launches, 2);
});


test('progressive actual controller keeps a newly opened Notes editor after a late optional-source network error', async () => {
  const f = fixture(), communities = deferred(), original = f.app.api.request;
  let fieldDisposed = 0, notesDisposed = 0;
  const noteScreen = new ElementPort();
  const notes = { dispose() { notesDisposed++; }, hasUnsavedChanges() { return false; } };
  // Rendering ports supply only owned UI resources; real renderCurrent,
  // navigateReady and cleanScreen retain all controller lifecycle decisions.
  f.app.renderCurrent = f.Controller.prototype.renderCurrent;
  f.app.renderUnifiedField = function () {
    this.unifiedField = { async refresh() {}, dispose() { fieldDisposed++; },
      hasUnsavedChanges() { return false; }, reconnect() {} };
  };
  f.app.renderNotes = function () { this.notesHandle = notes; this.main.replaceChildren(noteScreen); };
  f.app.preferences = {};
  f.app.persist = () => {};
  f.app.communities = [];
  f.app.api.request = (op, args) => op === 'world.community.list' ? communities.promise : original(op, args);
  const refreshing = f.app.refresh();
  try {
    await turn();
    const fieldShownBeforeOptional = Boolean(f.app.unifiedField);
    const fieldScreen = f.app.screenSequence;
    f.app.navigateReady('notes');
    const editorScreen = f.app.screenSequence;
    communities.reject(failure('NETWORK_ERROR'));
    await refreshing;
    assert.equal(fieldShownBeforeOptional, true);
    assert.ok(editorScreen > fieldScreen, 'real navigation increments the screen generation');
    assert.equal(fieldDisposed, 1, 'the field is disposed only by the intentional navigation');
    assert.equal(notesDisposed, 0);
    assert.equal(f.app.notesHandle, notes);
    assert.equal(f.app.main.children[0], noteScreen);
    assert.equal(f.app.screenSequence, editorScreen);
    assert.equal(f.location.hash, '#notes');
    assert.equal(f.app.homeStatus.communities, 'error');
  } finally {
    communities.resolve({ communities: [] });
    await refreshing;
    f.app.cleanScreen();
  }
});

test('progressive actual refresh and openRoute retain a cold conversation link until membership is available', async () => {
  const id = 'chat-cold-requested';
  const f = fixture({ hash: `#messages/${id}` }), communities = deferred(), original = f.app.api.request;
  const navigations = [];
  f.app.routeLoaded = false;
  f.app.communities = [];
  f.app.openRoute = f.Controller.prototype.openRoute;
  // Observe the navigation boundary instead of copying renderMessages selection
  // logic. Dispatching while membership is pending is the regression trigger.
  f.app.navigate = function (view, chatId) {
    navigations.push({ view, chatId, status: this.homeStatus.communities,
      membershipKnown: this.communities.some(value => value.communityId === chatId) });
  };
  f.app.api.request = (op, args) => op === 'world.community.list' ? communities.promise : original(op, args);
  const refreshing = f.app.refresh();
  try {
    await turn();
    const earlyNavigations = navigations.length;
    const earlyRoute = f.location.hash;
    communities.resolve({ communities: [{ communityId: id, membership: { state: 'active' } }] });
    await refreshing;
    assert.equal(earlyNavigations, 0, 'do not resolve a cold message route against an empty pending list');
    assert.equal(earlyRoute, `#messages/${id}`);
    assert.deepEqual(navigations, [{ view: 'messages', chatId: id, status: 'ready', membershipKnown: true }]);
    assert.equal(f.location.hash, `#messages/${id}`);
    assert.equal(f.app.routeLoaded, true);
  } finally {
    communities.resolve({ communities: [] });
    await refreshing;
    f.app.cleanScreen();
  }
});

// Progressive transport permits a read to finish after a later accepted write.
// Execute the actual full controller and exact production closures below.
function cacheCommunity({ revision = 1, membershipRevision = 1, role = 'owner', name = 'Older community' } = {}) {
  return { communityId: 'cache-group', name, revision, description: '', topics: [], joinPolicy: 'open', showcase: '', symbol: 'people', color: 'honey',
    memberCount: 3, previewMembers: [], unreadCount: 2, membership: { state: 'active', role, pinned: false, muted: false, showInProfile: true, revision: membershipRevision },
    permissions: { canManage: role === 'owner', canModerate: role === 'owner', canWrite: true } };
}
const cacheAst = ts.createSourceFile('app.ts', source, ts.ScriptTarget.Latest, true);
function cacheFind(root, predicate) {
  if (predicate(root)) return root;
  for (const child of root.getChildren(cacheAst)) { const found = cacheFind(child, predicate); if (found) return found; }
  return null;
}
function cacheBindArrow(f, node, parameters = [], values = [], declarations = '') {
  assert.ok(node && ts.isArrowFunction(node), 'The actual production arrow exists');
  const exported = {};
  const code = ts.transpileModule('exports.bind = function(' + parameters.join(',') + ') { ' + declarations + '; return ' + node.getText(cacheAst) + '; };', {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
  }).outputText;
  vm.runInNewContext(code, { exports: exported });
  return exported.bind.call(f.app, ...values);
}
function cacheLeave(f, group, dialog) {
  const request = cacheFind(cacheAst, node => ts.isCallExpression(node) && node.arguments.length && ts.isStringLiteral(node.arguments[0]) && node.arguments[0].text === 'world.membership.leave');
  assert.ok(request, 'The genuine leave request exists');
  let arrow = request.parent; while (arrow && !ts.isArrowFunction(arrow)) arrow = arrow.parent;
  return cacheBindArrow(f, arrow, ['group', 'dialog', 'accountCurrent'], [group, dialog, f.app.accountTask()]);
}
function cacheInboxEvent(f) {
  const registration = cacheFind(cacheAst, node => ts.isCallExpression(node) && node.arguments.length && ts.isStringLiteral(node.arguments[0]) && node.arguments[0].text === 'soty:inbox-updated');
  return cacheBindArrow(f, registration?.arguments[1]);
}

test('full refresh cannot roll back an accepted profile rename while a newer server revision still replaces it', async () => {
  for (const incomingRevision of [1, 3]) {
    const f = fixture(), pending = deferred(), original = f.app.api.request;
    f.app.view = 'notes'; f.app.profile = { profileId: 'account-A', displayName: 'Before', revision: 1 };
    f.app.api.request = (op, args) => {
      if (op === 'world.profile.get') return pending.promise;
      if (op === 'world.profile.update') {
        assert.equal(args.expectedRevision, 1); assert.equal(args.displayName, 'Accepted rename');
        return Promise.resolve({ profile: { profileId: 'account-A', displayName: 'Accepted rename', revision: 2 } });
      }
      return original(op, args);
    };
    const refresh = f.app.refresh(); await turn();
    await f.app.updateProfile({ displayName: 'Accepted rename' });
    assert.equal(f.app.profile.revision, 2); assert.equal(f.app.profile.displayName, 'Accepted rename');
    pending.resolve({ profile: { profileId: 'account-A', displayName: 'Read revision ' + incomingRevision, revision: incomingRevision } });
    await refresh;
    assert.equal(f.app.profile.revision, Math.max(2, incomingRevision));
    assert.equal(f.app.profile.displayName, incomingRevision < 2 ? 'Accepted rename' : 'Read revision 3');
    assert.equal(f.app.deskAccount, 'account-A'); assert.equal(f.app.accountGeneration, 1);
    f.app.cleanScreen();
  }
});

test('full refresh preserves a newer accepted membership projection and community name after an older whole-list reply', async () => {
  const f = fixture(), pending = deferred(), original = f.app.api.request;
  const older = cacheCommunity(), accepted = cacheCommunity({ revision: 2, membershipRevision: 2, role: 'member', name: 'Accepted current name' });
  f.app.view = 'world'; f.app.group = older; f.app.communities = [older]; f.app.results.communities = [older];
  f.app.openGroup = async () => {}; f.app.renderGroup = () => {};
  f.app.api.request = (op, args) => op === 'world.community.list' ? pending.promise
    : op === 'world.membership.preferences' ? Promise.resolve({ community: accepted }) : original(op, args);
  const refresh = f.app.refresh(); await turn();
  await f.app.mutateMembership('world.membership.preferences', { pinned: true });
  assert.equal(f.app.communities[0].membership.role, 'member'); assert.equal(f.app.communities[0].name, accepted.name);
  pending.resolve({ communities: [older] }); await refresh;
  assert.equal(f.app.communities[0].membership.revision, 2); assert.equal(f.app.communities[0].membership.role, 'member');
  assert.equal(f.app.communities[0].permissions.canManage, false); assert.equal(f.app.communities[0].name, accepted.name);
  assert.equal(f.app.results.communities[0].name, accepted.name); assert.equal(f.app.homeStatus.communities, 'ready');
  f.app.cleanScreen();
});

test('full refresh cannot restore a community removed by its actual accepted leave callback', async () => {
  const f = fixture(), pending = deferred(), original = f.app.api.request, older = cacheCommunity({ role: 'member' });
  let closed = 0, navigated = 0, leaves = 0;
  f.app.view = 'world'; f.app.group = older; f.app.communities = [older]; f.app.openGroup = async () => {};
  f.app.navigate = () => { navigated++; };
  f.app.api.request = (op, args) => op === 'world.community.list' ? pending.promise
    : op === 'world.membership.leave' ? (leaves++, assert.equal(args.communityId, older.communityId), Promise.resolve({})) : original(op, args);
  const refresh = f.app.refresh(); await turn();
  await cacheLeave(f, older, { close() { closed++; } })();
  assert.equal(leaves, 1); assert.equal(closed, 1); assert.equal(navigated, 1); assert.equal(f.app.communities.length, 0);
  pending.resolve({ communities: [older] }); await refresh;
  assert.equal(f.app.communities.length, 0, 'A successful old list cannot revive confirmed leave absence');
  f.app.cleanScreen();
});

test('message navigation cannot overwrite a newer point acknowledgement with its held whole-list reply', async () => {
  const f = fixture(), pending = deferred(), older = cacheCommunity(), accepted = cacheCommunity({ membershipRevision: 2, role: 'member' });
  f.app.communities = [older]; f.app.preferences = {}; f.app.persist = () => {};
  let rendered = 0; f.app.renderMessages = () => { rendered++; };
  f.app.api.request = op => { assert.equal(op, 'world.community.list'); return pending.promise; };
  f.app.navigateReady('messages'); f.app.updateCommunity(accepted);
  pending.resolve({ communities: [older] }); await turn(); await turn();
  assert.equal(f.app.communities[0].membership.role, 'member'); assert.equal(f.app.communities[0].membership.revision, 2);
  assert.equal(rendered, 0, 'The retired list cannot rebuild the conversation selection');
  f.app.cleanScreen();
});

test('full personal loader keeps a newer accepted community while still settling its other resources', async () => {
  const f = fixture(), pending = deferred(), older = cacheCommunity(), accepted = cacheCommunity({ membershipRevision: 2, role: 'member' });
  f.app.communities = [older]; const devices = f.app.devices, apps = f.app.apps;
  f.app.api.request = op => { assert.equal(op, 'world.community.list'); return pending.promise; };
  const loading = f.app.loadPersonal('communities'); f.app.updateCommunity(accepted);
  pending.resolve({ communities: [older] }); await loading;
  assert.equal(f.app.communities[0].membership.role, 'member'); assert.equal(f.app.communities[0].membership.revision, 2);
  assert.equal(f.app.devices, devices); assert.equal(f.app.apps, apps); assert.equal(f.app.homeStatus.communities, 'ready');
  f.app.cleanScreen();
});

test('actual inbox refresh closure retires its held list after a newer controller acknowledgement', async () => {
  const f = fixture(), pending = deferred(), older = cacheCommunity(), accepted = cacheCommunity({ membershipRevision: 2, role: 'member' });
  const events = [], groups = [older];
  f.app.communities = [older]; f.app.api.request = op => { assert.equal(op, 'world.community.list'); return pending.promise; };
  const declaration = cacheFind(cacheAst, node => ts.isVariableDeclaration(node) && node.name.getText(cacheAst) === 'refreshInbox');
  const parameters = ['active', 'selectedUpdate', 'document', 'groups', 'selectedId', 'visibleConversation', 'pendingPreviews', 'unsubscribers', 'previews', 'conversationRows',
    'subscribeDrafts', 'renderList', 'startPreviews', 'screen', 'room', 'CustomEvent', 'refreshDirect'];
  const inert = () => {};
  const values = [() => true, 0, { visibilityState: 'visible' }, groups, undefined, () => false, new Set(), new Map(), new Map(), new Map(),
    inert, inert, inert, { dispatchEvent(event) { events.push(event); } }, { querySelector() { return null; } }, class { constructor(type, init) { this.type = type; Object.assign(this, init); } }, inert];
  const refreshInbox = cacheBindArrow(f, declaration?.initializer, parameters, values, 'let refreshing = false');
  const refreshing = refreshInbox(); f.app.updateCommunity(accepted);
  pending.resolve({ communities: [older] }); await refreshing;
  assert.equal(f.app.communities[0].membership.role, 'member'); assert.equal(f.app.communities[0].membership.revision, 2);
  assert.equal(events.length, 0, 'A retired poll must not broadcast its old roles to widgets');
  assert.equal(groups[0], older, 'The retired poll must not rebuild its conversation collection');
  f.app.cleanScreen();
});

test('actual bubbled inbox handler only updates unread counts for existing IDs and cannot restore old roles or a left community', async () => {
  const f = fixture(), older = cacheCommunity(), accepted = cacheCommunity({ membershipRevision: 2, role: 'member', name: 'Accepted current name' });
  const unknown = { ...older, communityId: 'unknown-old-widget-group' };
  let notifications = 0; f.app.updateNavUnread = () => { notifications++; };
  f.app.communities = [older]; f.app.updateCommunity(accepted);
  const event = cacheInboxEvent(f);
  event({ detail: { communities: [{ ...older, unreadCount: 11 }, unknown] } });
  assert.equal(f.app.communities.length, 1); assert.equal(f.app.communities[0].communityId, accepted.communityId);
  assert.equal(f.app.communities[0].membership.role, 'member'); assert.equal(f.app.communities[0].name, accepted.name);
  assert.equal(f.app.communities[0].unreadCount, 11); assert.equal(notifications, 1);
  f.app.api.request = op => { assert.equal(op, 'world.membership.leave'); return Promise.resolve({}); };
  f.app.navigate = () => {};
  await cacheLeave(f, accepted, { close() {} })();
  event({ detail: { communities: [older, unknown] } });
  assert.equal(f.app.communities.length, 0); assert.equal(notifications, 2);
  f.app.cleanScreen();
});

test('a fresh whole-list read dispatched after an acknowledgement can still replace the collection normally', async () => {
  const f = fixture(), original = f.app.api.request;
  const accepted = cacheCommunity({ membershipRevision: 2, role: 'member' }), fresh = cacheCommunity({ revision: 3, membershipRevision: 3, role: 'moderator', name: 'Current server list' });
  const added = { ...fresh, communityId: 'freshly-found-community' };
  f.app.view = 'notes'; f.app.communities = []; f.app.updateCommunity(accepted);
  f.app.api.request = (op, args) => op === 'world.community.list' ? Promise.resolve({ communities: [fresh, added] }) : original(op, args);
  await f.app.refresh();
  assert.deepEqual(f.app.communities.map(value => value.communityId), [fresh.communityId, added.communityId]);
  assert.equal(f.app.communities[0].membership.revision, 3); assert.equal(f.app.communities[0].name, fresh.name);
  f.app.cleanScreen();
});

test('the actual leave acknowledgement cannot mutate a controller after account A to B to A', async () => {
  const f = fixture(), pending = deferred(), group = cacheCommunity({ role: 'member' }); let closed = 0, navigated = 0;
  f.app.communities = [group]; f.app.navigate = () => { navigated++; };
  f.app.api.request = op => { assert.equal(op, 'world.membership.leave'); return pending.promise; };
  const leaving = cacheLeave(f, group, { close() { closed++; } })();
  f.app.transitionAccount('account-B'); f.app.transitionAccount('account-A'); pending.resolve({}); await leaving;
  assert.equal(closed, 0); assert.equal(navigated, 0); assert.equal(f.app.communities.length, 0);
  f.app.cleanScreen();
});

// Two successful list reads can now overlap without a local point mutation.
// Keep the real controller/production closures and model only owned UI ports.
function ticketInbox(f) {
  const sequence = f.app.screenSequence, current = f.app.accountTask(), groups = f.app.communities.filter(group => group.membership?.state === 'active');
  const screen = { isConnected: true, dispatchEvent() {} };
  const declaration = cacheFind(cacheAst, node => ts.isVariableDeclaration(node) && node.name.getText(cacheAst) === 'refreshInbox');
  const parameters = ['active', 'selectedUpdate', 'document', 'groups', 'selectedId', 'visibleConversation', 'pendingPreviews', 'unsubscribers', 'previews', 'conversationRows',
    'subscribeDrafts', 'renderList', 'startPreviews', 'screen', 'room', 'CustomEvent', 'refreshDirect'];
  const inert = () => {};
  const values = [() => !f.app.destroyed && current() && sequence === f.app.screenSequence && screen.isConnected, 0, { visibilityState: 'visible' }, groups, undefined, () => false,
    new Set(), new Map(), new Map(), new Map(), inert, inert, inert, screen, { querySelector() { return null; } },
    class { constructor(type, init) { this.type = type; Object.assign(this, init); } }, inert];
  return cacheBindArrow(f, declaration?.initializer, parameters, values, 'let refreshing = false');
}
function ticketField(f) {
  f.app.view = 'mine';
  f.app.unifiedField = { setMode() {}, async refresh() {}, dispose() {}, reconnect() {}, hasUnsavedChanges() { return false; } };
  f.app.renderCurrent = f.Controller.prototype.renderCurrent;
}
function ticketOwnerWindow(f) {
  f.app.appSettingsDialog = { element: { open: true }, close() { this.element.open = false; } };
}
function ticketFixture() {
  const f = fixture(), older = cacheCommunity(), newer = cacheCommunity({ revision: 2, membershipRevision: 2, role: 'member', name: 'Newer confirmed list' });
  const gates = [deferred(), deferred()], calls = [];
  f.app.communities = [older]; f.app.profile = { profileId: 'account-A', displayName: 'Current A', revision: 1 };
  f.app.preferences = {}; f.app.persist = () => {}; f.app.renderMessages = () => {};
  f.app.api.request = (op, args) => {
    if (op === 'world.profile.get') return Promise.resolve({ profile: { profileId: 'account-A', displayName: 'Current A', revision: 1 } });
    assert.equal(op, 'world.community.list'); assert.ok(calls.length < gates.length, 'Only the two genuine list consumers may dispatch');
    const gate = gates[calls.length]; calls.push({ op, args }); return gate.promise;
  };
  return { ...f, older, newer, gates, calls };
}
async function ticketPair(f, olderConsumer) {
  let older, newer;
  if (olderConsumer === 'refresh') {
    f.app.view = 'messages'; older = f.app.refresh(); await turn();
    newer = ticketInbox(f)();
  } else if (olderConsumer === 'navigation') {
    f.app.navigateReady('messages'); older = Promise.resolve();
    ticketOwnerWindow(f); newer = f.app.refresh();
  } else if (olderConsumer === 'personal') {
    ticketField(f); older = f.app.loadPersonal('communities'); newer = f.app.refresh();
  } else {
    f.app.view = 'messages'; older = ticketInbox(f)();
    ticketOwnerWindow(f); newer = f.app.refresh();
  }
  await turn(); await turn(); assert.equal(f.calls.length, 2);
  return { older, newer };
}
async function ticketFinish(promise) { await promise; await turn(); await turn(); }
function ticketAssertSnapshot(f, expected) {
  assert.deepEqual(f.app.communities.map(group => group.communityId), expected.map(group => group.communityId));
  if (expected.length) {
    assert.equal(f.app.communities[0].name, expected[0].name);
    assert.equal(f.app.communities[0].membership.revision, expected[0].membership.revision);
    assert.equal(f.app.communities[0].membership.role, expected[0].membership.role);
    assert.equal(f.app.communities[0].permissions.canManage, expected[0].permissions.canManage);
  }
}

for (const olderConsumer of ['refresh', 'navigation', 'personal', 'inbox']) {
  for (const order of ['newer first', 'older first']) {
    test('community read ticket preserves the newer successful list for ' + olderConsumer + ' with ' + order, async () => {
      const f = ticketFixture();
      const next = ['refresh', 'personal'].includes(olderConsumer) ? [] : [f.newer];
      let pair;
      try {
        pair = await ticketPair(f, olderConsumer);
        if (order === 'newer first') {
          f.gates[1].resolve({ communities: next }); await ticketFinish(pair.newer); ticketAssertSnapshot(f, next);
          f.gates[0].resolve({ communities: [f.older] }); await ticketFinish(pair.older);
        } else {
          f.gates[0].resolve({ communities: [f.older] }); await ticketFinish(pair.older); ticketAssertSnapshot(f, [f.older]);
          f.gates[1].resolve({ communities: next }); await ticketFinish(pair.newer);
        }
        ticketAssertSnapshot(f, next);
        assert.equal(f.app.deskAccount, 'account-A'); assert.equal(f.app.accountGeneration, 1);
      } finally {
        f.gates.forEach(gate => gate.resolve({ communities: [] }));
        if (pair) await Promise.allSettled([pair.older, pair.newer]); await turn(); f.app.cleanScreen();
      }
    });
  }
}

for (const order of ['newer first', 'older first']) {
  test('community read tickets retain the independent point-ack fence with ' + order, async () => {
    const f = ticketFixture(), accepted = cacheCommunity({ revision: 3, membershipRevision: 3, role: 'moderator', name: 'Accepted point acknowledgement' });
    let pair;
    try {
      pair = await ticketPair(f, 'personal'); f.app.updateCommunity(accepted);
      if (order === 'newer first') {
        f.gates[1].resolve({ communities: [f.newer] }); await ticketFinish(pair.newer);
        f.gates[0].resolve({ communities: [f.older] }); await ticketFinish(pair.older);
      } else {
        f.gates[0].resolve({ communities: [f.older] }); await ticketFinish(pair.older);
        f.gates[1].resolve({ communities: [f.newer] }); await ticketFinish(pair.newer);
      }
      ticketAssertSnapshot(f, [accepted]);
      assert.equal(f.app.deskAccount, 'account-A'); assert.equal(f.app.accountGeneration, 1);
    } finally {
      f.gates.forEach(gate => gate.resolve({ communities: [] }));
      if (pair) await Promise.allSettled([pair.older, pair.newer]); f.app.cleanScreen();
    }
  });

  test('community read tickets retain account ABA rejection and allow a later current read with ' + order, async () => {
    const f = ticketFixture(); let pair;
    try {
      pair = await ticketPair(f, 'personal');
      f.app.transitionAccount('account-B'); f.app.transitionAccount('account-A');
      if (order === 'newer first') {
        f.gates[1].resolve({ communities: [f.newer] }); await ticketFinish(pair.newer);
        f.gates[0].resolve({ communities: [f.older] }); await ticketFinish(pair.older);
      } else {
        f.gates[0].resolve({ communities: [f.older] }); await ticketFinish(pair.older);
        f.gates[1].resolve({ communities: [f.newer] }); await ticketFinish(pair.newer);
      }
      assert.equal(f.app.communities.length, 0); assert.equal(f.app.profile, null);
      f.app.view = 'notes'; f.app.renderCurrent = () => {};
      f.app.api.request = op => op === 'world.profile.get' ? Promise.resolve({ profile: { profileId: 'account-A', displayName: 'Current after ABA', revision: 1 } })
        : (assert.equal(op, 'world.community.list'), Promise.resolve({ communities: [f.newer] }));
      await f.app.refresh(); ticketAssertSnapshot(f, [f.newer]); assert.equal(f.app.profile.profileId, 'account-A');
    } finally {
      f.gates.forEach(gate => gate.resolve({ communities: [] }));
      if (pair) await Promise.allSettled([pair.older, pair.newer]); f.app.cleanScreen();
    }
  });
}

test('a devices-only personal refresh cannot restore its cached community array over a later successful real list', async () => {
  const f = ticketFixture(), devices = deferred(); ticketField(f);
  f.app.options.listDevices = () => devices.promise;
  f.app.api.request = op => op === 'world.profile.get' ? Promise.resolve({ profile: { profileId: 'account-A', displayName: 'Current A', revision: 1 } })
    : (assert.equal(op, 'world.community.list'), Promise.resolve({ communities: [f.newer] }));
  const personal = f.app.loadPersonal('devices');
  try {
    await f.app.refresh(); ticketAssertSnapshot(f, [f.newer]);
    devices.resolve([{ deviceId: 'current-device' }]); await personal;
    ticketAssertSnapshot(f, [f.newer]); assert.equal(f.app.devices[0].deviceId, 'current-device');
  } finally { devices.resolve([]); await personal; f.app.cleanScreen(); }
});

test('a failed newer community read does not retire an earlier successful list that is still available', async () => {
  const f = ticketFixture(), available = cacheCommunity({ revision: 2, membershipRevision: 2, role: 'member', name: 'Available earlier response' });
  let pair;
  try {
    pair = await ticketPair(f, 'navigation');
    f.gates[1].reject(failure('NETWORK_ERROR')); await ticketFinish(pair.newer);
    f.gates[0].resolve({ communities: [available] }); await ticketFinish(pair.older);
    ticketAssertSnapshot(f, [available]); assert.equal(f.app.deskAccount, 'account-A');
  } finally {
    f.gates.forEach(gate => gate.resolve({ communities: [] }));
    if (pair) await Promise.allSettled([pair.older, pair.newer]); await turn(); f.app.cleanScreen();
  }
});


// These callbacks come from the real WorldApplication methods compiled above.
// Reuse the established platform ports; only the full Notes renderer is outside
// this test's boundary. Actual openNotes/writeRoute/cleanScreen/flush/account
// transition methods run. No copied menu callback or copied exit guard.
const notesMenuEntries = [
  { name: 'Work / Notes', method: 'openWorkMenu', label: 'Записки', noteId: null, route: '#notes' },
  { name: 'Create / Note', method: 'openAddMenu', label: 'Записку', noteId: 'new', route: '#notes/new' },
];
function notesMenuDraftFixture(entry) {
  const f = fixture({ hash: '#notes/current-note' }), pending = deferred(), mounts = [];
  let dirty = true, flushes = 0, disposals = 0;
  const draft = {
    hasUnsavedChanges: () => dirty,
    flush() { flushes++; return pending.promise; },
    dispose() { disposals++; },
  };
  Object.assign(f.app, {
    view: 'notes', preferences: { view: 'notes' }, notesHandle: draft,
    unifiedField: null,
    // Restore the actual method: fixture's general read-only openNotes port is
    // deliberately insufficient for proving destructive route/cleanup behavior.
    openNotes: f.Controller.prototype.openNotes,
    persist() {},
    renderNotes(noteId) { mounts.push({ accountId: this.deskAccount, noteId: noteId ?? null }); },
  });
  f.app.main.append(new ElementPort());
  f.app[entry.method]();
  const menu = f.dialogs.at(-1);
  const buttons = node => [node, ...node.children.flatMap(child => child instanceof ElementPort ? buttons(child) : [])]
    .filter(child => child.tagName === 'button');
  const matches = buttons(menu.body).filter(button => button.textContent === entry.label);
  assert.equal(matches.length, 1, 'click one actual menu entry, selected by its visible label');
  return { ...f, pending, draft, mounts, menu,
    click() { matches[0].emit('click'); },
    setDurable() { dirty = false; },
    get flushes() { return flushes; }, get disposals() { return disposals; },
  };
}
for (const entry of notesMenuEntries) {
  test(`actual Notes menu draft guard: ${entry.name} clean positive reaches real Notes route exactly once`, () => {
    const f = notesMenuDraftFixture(entry), sequence = f.app.screenSequence;
    f.setDurable(); f.click();
    assert.equal(f.flushes, 0);
    assert.equal(f.menu.element.open, false);
    assert.equal(f.location.hash, entry.route);
    assert.equal(f.app.activeRoute, entry.route);
    assert.equal(f.app.screenSequence, sequence + 1);
    assert.equal(f.disposals, 1);
    assert.deepEqual(f.mounts, [{ accountId: 'account-A', noteId: entry.noteId }]);
  });
  test(`actual Notes menu draft guard: ${entry.name} holds a dirty editor until successful flush and mounts once`, async () => {
    const f = notesMenuDraftFixture(entry), sequence = f.app.screenSequence, route = f.location.hash;
    f.click(); f.click();
    assert.equal(f.flushes, 1, 'repeated activation shares the existing pending exit');
    assert.equal(f.menu.element.open, false);
    assert.equal(f.location.hash, route);
    assert.equal(f.app.screenSequence, sequence);
    assert.equal(f.app.notesHandle, f.draft);
    assert.equal(f.disposals, 0);
    assert.equal(f.mounts.length, 0);
    f.setDurable(); f.pending.resolve(); await turn();
    assert.equal(f.location.hash, entry.route);
    assert.equal(f.app.screenSequence, sequence + 1);
    assert.equal(f.disposals, 1);
    assert.equal(f.app.noteActionPending, false);
    assert.deepEqual(f.mounts, [{ accountId: 'account-A', noteId: entry.noteId }]);
  });
  test(`actual Notes menu draft guard: ${entry.name} rejected local flush preserves draft and current route`, async () => {
    const f = notesMenuDraftFixture(entry), sequence = f.app.screenSequence, route = f.location.hash;
    f.click();
    assert.equal(f.flushes, 1);
    f.pending.reject(failure('notes_local_unavailable')); await turn();
    assert.equal(f.location.hash, route);
    assert.equal(f.app.screenSequence, sequence);
    assert.equal(f.app.notesHandle, f.draft);
    assert.equal(f.disposals, 0);
    assert.equal(f.mounts.length, 0);
    assert.equal(f.app.noteActionPending, false);
    assert.match(f.app.lastToast, /не сохранены/u);
  });
  test(`actual Notes menu draft guard: ${entry.name} resolved flush with undurable data still cannot leave`, async () => {
    const f = notesMenuDraftFixture(entry), sequence = f.app.screenSequence, route = f.location.hash;
    f.click(); f.pending.resolve(); await turn();
    assert.equal(f.flushes, 1);
    assert.equal(f.location.hash, route);
    assert.equal(f.app.screenSequence, sequence);
    assert.equal(f.app.notesHandle, f.draft);
    assert.equal(f.disposals, 0);
    assert.equal(f.mounts.length, 0);
    assert.match(f.app.lastToast, /сохраните/u);
  });
  for (const accounts of [['account-B'], ['account-B', 'account-A']]) {
    test(`actual Notes menu draft guard: ${entry.name} held exit cannot navigate after ${accounts.join(' -> ')}`, async () => {
      const f = notesMenuDraftFixture(entry), generation = f.app.accountGeneration;
      f.click(); assert.equal(f.flushes, 1);
      for (const account of accounts) f.app.transitionAccount(account);
      assert.ok(f.app.accountGeneration > generation);
      assert.equal(f.app.deskAccount, accounts.at(-1));
      assert.equal(f.disposals, 1, 'the real account transition disposes the previous owner once');
      const currentSequence = f.app.screenSequence, currentRoute = f.location.hash;
      f.setDurable(); f.pending.resolve(); await turn();
      assert.equal(f.app.screenSequence, currentSequence);
      assert.equal(f.location.hash, currentRoute);
      assert.equal(f.app.notesHandle, null);
      assert.equal(f.disposals, 1);
      assert.equal(f.mounts.length, 0, 'the old menu must not mount Notes in the current account generation');
      assert.equal(f.app.lastToast, undefined);
    });
  }
}

// Append to a FRESH candidate copy of app-account-lifecycle.test.mjs after Source fixes.
// Uses the existing actual-controller fixture, conversationTargets and AST closure
// binder. Controlled API/navigation ports model response order only. These cases
// are NOT signed-service, browser, layout or network evidence, and were NOT run
// by this reviewer. Do not append to an immutable packet or logs.
const reviewThread = (peer = 'peer-one', relationship = 'rel-one', unread = 7) => ({
  schema: 'soty.world-direct.v1', threadId: 'direct_' + peer, revision: 1,
  relationshipId: relationship, peer: { accountId: peer, label: null, avatarRevision: null },
  unreadCount: unread, preferences: { muted: false, pinned: false }, createdAt: 1,
});
const reviewContact = peer => ({ peerAccountId: peer, relationshipId: 'rel-' + peer, label: peer });
function reviewDirectOpenFixture() {
  const f = fixture(), navigation = [], errors = [], gates = new Map([['peer-A', deferred()], ['peer-B', deferred()]]);
  f.app.profile.directMessagesVersion = 1; f.app.directOpenSequence = 0;
  f.app.api = { request(op, args) { assert.equal(op, 'world.direct.open'); return gates.get(args.peerAccountId).promise; } };
  f.app.navigate = (view, id) => { navigation.push({ view, id }); f.app.screenSequence++; };
  f.app.toast = value => errors.push(value);
  const reply = peer => ({ current: reviewThread(peer, 'rel-' + peer) });
  return { f, gates, navigation, errors, reply };
}
async function reviewLatestOpenWins(first) {
  const { f, gates, navigation, reply } = reviewDirectOpenFixture();
  const older = f.app.openDirectContact(reviewContact('peer-A')), latest = f.app.openDirectContact(reviewContact('peer-B'));
  gates.get(first).resolve(reply(first)); await turn();
  const other = first === 'peer-A' ? 'peer-B' : 'peer-A'; gates.get(other).resolve(reply(other));
  await Promise.all([older, latest]);
  assert.deepEqual(navigation, [{ view: 'messages', id: 'direct_peer-B' }]);
  assert.equal(f.app.directLabels.has('peer-A'), false, 'superseded opens do not change the new screen cache');
}
test('review F1 latest direct intent wins when the older response arrives first', async () => { await reviewLatestOpenWins('peer-A'); });
test('review F1 latest direct intent wins when the latest response arrives first', async () => { await reviewLatestOpenWins('peer-B'); });
test('review F1 a departed screen is not replaced by a late direct-open response', async () => {
  const { f, gates, navigation, reply } = reviewDirectOpenFixture();
  const opening = f.app.openDirectContact(reviewContact('peer-A')); f.app.screenSequence++;
  gates.get('peer-A').resolve(reply('peer-A')); await opening;
  assert.deepEqual(navigation, []); assert.equal(f.app.directThreads.length, 0); assert.equal(f.app.directLabels.size, 0);
});
test('review F1 a superseded direct-open failure does not surface on the selected screen', async () => {
  const { f, gates, navigation, errors, reply } = reviewDirectOpenFixture();
  const older = f.app.openDirectContact(reviewContact('peer-A')), latest = f.app.openDirectContact(reviewContact('peer-B'));
  gates.get('peer-B').resolve(reply('peer-B')); await latest;
  gates.get('peer-A').reject(failure('NETWORK_ERROR')); await older;
  assert.equal(navigation.length, 1); assert.deepEqual(errors, []);
});

function reviewDirectPoll(f) {
  const declaration = cacheFind(cacheAst, node => ts.isVariableDeclaration(node) && node.name.getText(cacheAst) === 'refreshDirect');
  const inert = () => {};
  return cacheBindArrow(f, declaration?.initializer,
    ['directEnabled', 'active', 'document', 'accountId', 'renderList', 'refreshVisiblePreviews', 'pendingDirect', 'selectedDirect', 'selectedId', 'room', 'listStatus'],
    [true, () => true, { visibilityState: 'visible' }, 'account-A', inert, inert, false, undefined, undefined, { querySelector() { return null; } }, { textContent: '' }],
    'let directRefreshing = false');
}
function reviewMountedDirect(f, thread) {
  let target; f.app.updateNavUnread = () => {};
  f.app.mountConversation = (_parent, value) => { target = value; return () => true; };
  f.app.mountDirectChat(new ElementPort(), thread); return target;
}
test('review F2 actual direct poll cannot restore unread counts after the actual read-ACK closure', async () => {
  const f = fixture(), held = deferred(), before = reviewThread(), snapshot = structuredClone(before);
  f.app.directThreads = [before];
  f.app.api = { request(op) { if (op === 'world.direct.list') return held.promise; assert.equal(op, 'world.direct.chat.read'); return Promise.resolve({ unreadCount: 0 }); } };
  const target = reviewMountedDirect(f, before), polling = reviewDirectPoll(f)();
  const markRead = cacheFind(cacheAst, node => ts.isVariableDeclaration(node) && node.name.getText(cacheAst) === 'markRead');
  const ack = cacheBindArrow(f, markRead?.initializer,
    ['conversation', 'document', 'nearEnd', 'active', 'notifyConversation', 'closeAccess'],
    [target, { visibilityState: 'visible' }, () => true, () => true, () => {}, () => {}],
    'let reading = false, accessClosed = false, syncSeq = 9, lastRead = 0');
  ack(); await turn(); assert.equal(f.app.directThreads[0].unreadCount, 0);
  held.resolve({ threads: [snapshot], nextCursor: null }); await polling;
  assert.equal(f.app.directThreads[0].unreadCount, 0);
});
test('review F2 actual direct poll cannot erase a later accepted pinned preference', async () => {
  const f = fixture(), held = deferred(), before = reviewThread(), snapshot = structuredClone(before), accepted = structuredClone(before); accepted.preferences.pinned = true;
  f.app.directThreads = [before];
  f.app.api = { request(op) { if (op === 'world.direct.list') return held.promise; assert.equal(op, 'world.direct.preferences'); return Promise.resolve({ thread: accepted }); } };
  const target = reviewMountedDirect(f, before), polling = reviewDirectPoll(f)();
  await target.setPreference('pinned', true); assert.equal(f.app.directThreads[0].preferences.pinned, true);
  held.resolve({ threads: [snapshot], nextCursor: null }); await polling;
  assert.equal(f.app.directThreads[0].preferences.pinned, true);
});

test('review F3 a stale community get cannot revert a newer preference acknowledgement', async () => {
  const held = deferred(), before = cacheCommunity(), accepted = cacheCommunity({ membershipRevision: 2 }); accepted.membership.pinned = true;
  const target = conversationTargets.createCommunityConversation({ community: before, api: { request(op) {
    if (op === 'world.community.get') return held.promise; assert.equal(op, 'world.membership.preferences'); return Promise.resolve({ community: accepted });
  } } });
  const refreshing = target.refresh(); await target.setPreference('pinned', true);
  assert.equal(target.preferences.pinned, true); held.resolve({ community: structuredClone(before) }); await refreshing;
  assert.equal(target.preferences.pinned, true); assert.equal(target.record.membership.revision, 2);
});
test('review F3 a stale direct get cannot revert a newer preference with the same thread revision', async () => {
  const held = deferred(), before = reviewThread(), accepted = structuredClone(before); accepted.preferences.pinned = true;
  const target = conversationTargets.createDirectConversation({ thread: before, accountId: 'account-A', api: { request(op) {
    if (op === 'world.direct.get') return held.promise; assert.equal(op, 'world.direct.preferences'); return Promise.resolve({ thread: accepted });
  } } });
  const refreshing = target.refresh(); await target.setPreference('pinned', true);
  assert.equal(target.preferences.pinned, true); held.resolve({ thread: structuredClone(before) }); await refreshing;
  assert.equal(target.preferences.pinned, true);
});

test('actual QR application handoff preserves the one-use launch object after the draft fence', async () => {
  const f = fixture(), opened = [];
  f.app.chatDrafts = { hasVolatile: () => false };
  f.app.flushScreen = async () => {}; f.app.screenHasUnsavedChanges = () => false;
  f.app.appSettingsDialog = null; f.app.appPlacementDialog = null;
  const launch = appLaunch.parseAppLaunchRoute(`#app/app-${'a'.repeat(32)}?invitation=${'b'.repeat(64)}`);
  f.app.openApplication = async (record, intent) => opened.push({ record, intent });
  await f.app.followConnection({ kind: 'application', launch });
  assert.equal(opened.length, 1); assert.equal(opened[0].intent, launch);
  assert.equal(opened[0].intent.takeInvitation(), 'b'.repeat(64)); assert.equal(launch.takeInvitation(), null);
});
test('actual QR handoff cannot leave an undurable draft', async () => {
  const f = fixture(); let opened = 0;
  f.app.flushScreen = async () => {}; f.app.screenHasUnsavedChanges = () => true;
  f.app.openGroup = async () => { opened++; };
  await assert.rejects(f.app.followConnection({ kind: 'community', id: 'community-qa', tab: 'chat' }), /Сохраните/u);
  assert.equal(opened, 0);
});
test('actual QR handoff cannot adopt a changed account while the draft flush waits', async () => {
  const f = fixture(), held = deferred(); let opened = 0;
  f.app.flushScreen = () => held.promise; f.app.screenHasUnsavedChanges = () => false;
  f.app.openGroup = async () => { opened++; };
  const pending = f.app.followConnection({ kind: 'community', id: 'community-qa', tab: 'chat' });
  f.app.accountGeneration++; held.resolve();
  await assert.rejects(pending, /connection_entry_changed/); assert.equal(opened, 0);
});

for (const kind of ['platform-invitation','legacy-room']) test(`actual ${kind} QR handoff cannot consume navigation while a widget remains undurable`, async () => {
  const f=fixture();let consumed=0,dirty=true;
  f.app.chatDrafts = { hasVolatile: () => false };
  f.app.flushScreen=async()=>{};f.app.screenHasUnsavedChanges=()=>false;
  f.app.widgets={async flush(){},hasUnsavedChanges:()=>dirty};
  await assert.rejects(f.app.followConnection({kind},()=>{consumed++;}),/Сохраните/u);
  assert.equal(consumed,0);dirty=false;
  await f.app.followConnection({kind},()=>{consumed++;});assert.equal(consumed,1);
});


// Actual stage + launcher + voice carrier; only UI/HTTP/client ports are controlled.
const voiceMountFixture = (overrides = {}) => ({ scope: 'kvartalufa.voice', appId: stageAppId,
  parentOrigin: 'https://shell.example', frameOrigin: 'https://runtime.example', nativeOrigin: 'https://runtime.example',
  source: { id: 'voice-source', version: 1, digest: 'b'.repeat(64) }, ...overrides });
const voiceClientFixture = f => ({ async getLocalState() { return { accountId: f.app.deskAccount, deviceId: 'device-A', current: { revoked: false } }; },
  async extension() { throw failure('unexpected_voice_action'); } });
async function voiceUntil(predicate) { const deadline = Date.now() + 5000; while (!predicate()) { assert.ok(Date.now() < deadline, 'bounded voice stage continuation completed'); await turn(); } }
function voiceStageFixture(f, { readNativeVoice, nativeVoice, presentation = 'widget', host = f.app.main, reply = () => stageResponse('/soty-voice') } = {}) {
  const original = appLaunch.parseAppLaunchRoute(`#app/${stageAppId}?invitation=${'a'.repeat(64)}`), launches = [];
  let takes = 0;
  const intent = Object.freeze({ ...original, takeInvitation() { takes++; return original.takeInvitation(); } });
  const account = f.app.deskAccount, generation = f.app.accountGeneration, sequence = f.app.screenSequence;
  if (presentation !== 'widget') f.location.hash = '#' + intent.route;
  const stage = f.mountStage(host, { api: f.app.api, app: { appId: stageAppId, name: 'Voice' }, accountId: account, intent,
    ...(presentation === 'widget' ? { presentation, instanceId: 'widget-11111111-1111-4111-8111-111111111111' } : {}),
    ...(readNativeVoice ? { readNativeVoice } : {}), ...(nativeVoice ? { nativeVoice } : {}),
    isCurrent: () => !f.app.destroyed && f.app.deskAccount === account && f.app.accountGeneration === generation && f.app.screenSequence === sequence,
    async request(parameters) { launches.push(parameters); return reply(parameters); },
    onNavigate(next) { if (presentation !== 'widget') f.location.hash = '#' + next.route; }, onBack() {}, async onAccount() {}, onSettings() {},
  });
  return { stage, host, launches, takes: () => takes, original };
}
function voiceWidgetFromActualConstructor(f, intent) {
  const binder = {};
  vm.runInNewContext(ts.transpileModule(`exports.bind = function(mountAppStage, parseAppLaunchRoute, formatAppLaunchRoute) { return ${currentReviewWidgetMount}; };`,
    { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText, { exports: binder });
  const mount = binder.bind.call(f.app, f.mountStage, appLaunch.parseAppLaunchRoute, appLaunch.formatAppLaunchRoute);
  return mount(f.app.main, { appId: stageAppId, name: 'Voice' }, intent, () => !f.app.destroyed,
    'widget-11111111-1111-4111-8111-111111111111');
}
for (const presentation of ['main', 'widget']) test(`actual ${presentation} stage waits for voice metadata before launch, invitation consume and bridge install`, async t => {
  const f = fixture(), gate = deferred(); let reads = 0;
  const v = voiceStageFixture(f, { presentation, readNativeVoice: () => { reads++; return gate.promise; } }); t.after(() => v.stage.dispose());
  await turn();
  try {
    assert.equal(reads, 1); assert.equal(v.launches.length, 0); assert.equal(v.takes(), 0);
    assert.equal(v.host.querySelector('iframe'), null); assert.equal(f.nativeVoiceBridges.length, 0);
  } finally { gate.resolve({ client: voiceClientFixture(f), mount: voiceMountFixture() }); await v.stage.ready; }
  assert.equal(v.launches.length, 1); assert.equal(v.takes(), 0); assert.equal(f.nativeVoiceBridges.length, 1);
  const frame = v.host.querySelector('iframe'); assert.ok(frame);
  assert.equal(frame.allow, 'microphone https://runtime.example; autoplay https://runtime.example');
  assert.equal(frame.getAttribute('sandbox'), 'allow-scripts allow-forms allow-same-origin allow-downloads');
});
for (const transition of ['screen', 'account ABA', 'dispose']) test(`pending voice metadata cannot install or launch after ${transition}`, async t => {
  const f = fixture(), gate = deferred(), v = voiceStageFixture(f, { readNativeVoice: () => gate.promise }); t.after(() => v.stage.dispose());
  await turn();
  if (transition === 'screen') f.app.screenSequence++;
  else if (transition === 'account ABA') { f.app.transitionAccount('account-B'); f.app.transitionAccount('account-A'); }
  else v.stage.dispose();
  gate.resolve({ client: voiceClientFixture(f), mount: voiceMountFixture() }); await v.stage.ready;
  assert.equal(v.launches.length, 0); assert.equal(v.takes(), 0); assert.equal(f.nativeVoiceBridges.length, 0); assert.equal(v.host.querySelector('iframe'), null);
  assert.equal(v.original.takeInvitation(), 'a'.repeat(64), 'the actual cancelled carrier was not consumed');
});
for (const presentation of ['main', 'widget']) test(`actual ${presentation} voice stage rejects a foreign parent before signed launch or invitation consume`, async t => {
  const f = fixture(), v = voiceStageFixture(f, { presentation, readNativeVoice: async () => ({ client: voiceClientFixture(f), mount: voiceMountFixture({ parentOrigin: 'https://another.example' }) }) });
  t.after(() => v.stage.dispose()); await v.stage.ready;
  assert.equal(v.launches.length, 0); assert.equal(v.takes(), 0); assert.equal(v.host.querySelector('iframe'), null);
  assert.equal(v.original.takeInvitation(), 'a'.repeat(64));
});
test('legacy ready voice descriptor keeps immediate admission even when a lazy resolver is also supplied', async t => {
  const f = fixture(); let reads = 0;
  const v = voiceStageFixture(f, { nativeVoice: { client: voiceClientFixture(f), mount: voiceMountFixture() }, readNativeVoice: async () => { reads++; throw Error('ready descriptor must not wait'); } });
  t.after(() => v.stage.dispose()); assert.equal(v.launches.length, 1); await v.stage.ready;
  assert.equal(reads, 0); assert.equal(f.nativeVoiceBridges.length, 1); assert.ok(v.host.querySelector('iframe'));
});
test('actual main application entry receives the shared descriptor resolver', async t => {
  const f = fixture(), gate = deferred(); let reads = 0, launches = 0;
  f.app.options.readNativeVoice = () => { reads++; return gate.promise; };
  f.app.options.openApp = async () => { launches++; return stageResponse('/'); };
  const opening = f.app.openApplication({ appId: stageAppId, name: 'Voice' }); t.after(() => f.app.cleanScreen()); await turn();
  try { assert.equal(reads, 1); assert.equal(launches, 0); assert.equal(f.app.main.querySelector('iframe'), null); }
  finally { gate.resolve(null); await opening; }
  assert.equal(launches, 1); assert.ok(f.app.main.querySelector('iframe'));
});
test('actual constructor widget entry receives the shared descriptor resolver and legacy ready voice option', async t => {
  const f = fixture({ hash: '#world' }), gate = deferred(); let reads = 0, launches = 0;
  f.app.options.readNativeVoice = () => { reads++; return gate.promise; };
  f.app.options.openApp = async () => { launches++; return stageResponse('/'); };
  const intent = appLaunch.parseAppLaunchRoute(appLaunch.formatAppLaunchRoute({ appId: stageAppId }));
  const stage = voiceWidgetFromActualConstructor(f, intent); t.after(() => stage.dispose()); await turn();
  try { assert.equal(reads, 1); assert.equal(launches, 0); assert.equal(f.app.main.querySelector('iframe'), null); }
  finally { gate.resolve(null); await stage.ready; }
  assert.equal(launches, 1); assert.ok(f.app.main.querySelector('iframe'));
  stage.dispose(); f.app.options.nativeVoice = { client: voiceClientFixture(f), mount: voiceMountFixture() };
  const ready = voiceWidgetFromActualConstructor(f, intent); t.after(() => ready.dispose()); await ready.ready;
  assert.equal(reads, 1, 'legacy ready widget does not reread the pending metadata port'); assert.equal(f.nativeVoiceBridges.length, 1);
});
test('failed voice lookup retries only through explicit UI and two stages share the same retry IO', async t => {
  const f = fixture(), gate = deferred(); let calls = 0;
  const resolver = createNativeVoiceDescriptor({ fetch: async () => { calls++; if (calls === 1) throw Error('synthetic network'); return gate.promise; } });
  t.after(() => resolver.close()); await assert.rejects(resolver.read(), { code: 'native_voice_descriptor_unavailable' });
  const readNativeVoice = async options => { const mount = await resolver.read(options); return mount ? { client: voiceClientFixture(f), mount } : null; };
  const host = Object.assign(new ElementPort(), { ownerDocument: f.app.main.ownerDocument });
  const a = voiceStageFixture(f, { readNativeVoice }), b = voiceStageFixture(f, { readNativeVoice, host }); t.after(() => { a.stage.dispose(); b.stage.dispose(); });
  await Promise.all([a.stage.ready, b.stage.ready]); await turn();
  assert.equal(calls, 1); assert.equal(a.launches.length + b.launches.length, 0); assert.equal(a.takes() + b.takes(), 0);
  for (const v of [a, b]) { const retry = v.host.querySelector('.sa-recovery-retry'); assert.equal(retry.hidden, false); assert.equal(retry.disabled, false); retry.emit('click'); }
  assert.equal(calls, 2, 'concurrent explicit retries share one bounded metadata request');
  gate.resolve({ ok: true, async json() { return { nativeVoice: null }; } }); await voiceUntil(() => a.launches.length + b.launches.length === 2);
  assert.equal(calls, 2); assert.ok(a.host.querySelector('iframe')); assert.ok(b.host.querySelector('iframe'));
});
test('disposing one actual voice stage retires only its own bridge and leaves the other frame usable', async t => {
  const f = fixture(), client = voiceClientFixture(f), nativeVoice = { client, mount: voiceMountFixture() };
  const host = Object.assign(new ElementPort(), { ownerDocument: f.app.main.ownerDocument });
  const a = voiceStageFixture(f, { nativeVoice }), b = voiceStageFixture(f, { nativeVoice, host }); t.after(() => { a.stage.dispose(); b.stage.dispose(); });
  await Promise.all([a.stage.ready, b.stage.ready]); assert.equal(f.nativeVoiceBridges.length, 2);
  const first = a.host.querySelector('iframe'), second = b.host.querySelector('iframe'); assert.ok(first); assert.ok(second);
  const scope = (frame, requestId) => f.view.dispatchEvent(Object.assign(new Event('message'), { source: frame.contentWindow, origin: 'https://runtime.example', data: { schema: 'soty.kvartalufa-voice-carrier.v1', method: 'scope', requestId } }));
  scope(first, 'scope-first-111'); scope(second, 'scope-second-111'); await turn();
  assert.equal(first.contentWindow.sent.length, 1); assert.equal(second.contentWindow.sent.length, 1);
  a.stage.dispose(); a.stage.dispose(); assert.equal(f.nativeVoiceBridges[0].disposed, 1); assert.equal(f.nativeVoiceBridges[1].disposed, 0);
  scope(first, 'scope-first-222'); scope(second, 'scope-second-222'); await turn();
  assert.equal(first.contentWindow.sent.length, 1); assert.equal(second.contentWindow.sent.length, 2); assert.equal(b.host.querySelector('iframe'), second);
});


test('actual scoped stage gives an invitation only to its fresh admitted frame and never replays it', async t => {
  const f = fixture(), v = voiceStageFixture(f, { readNativeVoice: async () => null,
    reply: () => ({ ...stageResponse('/soty-voice'), runtimeProfile: 'soty.selected-human-embed.v2',
      scopedCloseHandle: 'h'.repeat(43), scopedSource: { id: 'soty.standard-workspace', version: 2, digest: 'a'.repeat(64) }, scopedCleanup: async () => {} }) });
  t.after(() => v.stage.dispose()); await v.stage.ready;
  const frame = v.host.querySelector('iframe'); assert.ok(frame); assert.equal(v.takes(), 0);
  const ready = { schema: 'soty.app-invitation-ready.v1', appId: stageAppId, requestId: 'c'.repeat(32) };
  const emit = (source, origin = 'https://runtime.example') => f.view.dispatchEvent(Object.assign(new Event('message'), { source, origin, data: ready }));
  emit({}); emit(frame.contentWindow, 'https://foreign.example'); assert.equal(v.takes(), 0);
  emit(frame.contentWindow); emit(frame.contentWindow); await turn();
  const delivered = frame.contentWindow.sent.filter(item => item.data.schema === 'soty.app-invitation.v1');
  assert.equal(v.takes(), 1); assert.equal(delivered.length, 1); assert.equal(delivered[0].data.token, 'a'.repeat(64));
  assert.equal(delivered[0].origin, 'https://runtime.example'); assert.equal(v.original.takeInvitation(), null);
});

// Independent P5 Source-review regression port. AppStage/launcher/carrier remain actual.
// Only browser UI, metadata and popup ports are controlled. Never a provider/hardware proof.
function independentP5Control(host, control) {
  const visit = node => {
    if (node?.dataset?.stageControl === control) return node;
    for (const child of node?.children ?? []) { const found = visit(child); if (found) return found; }
    return null;
  };
  const result = visit(host); assert.ok(result, `actual stage control ${control}`); return result;
}
function independentP5PopupPort(f) {
  const opened = [], navigations = [];
  f.view.open = (url, target) => {
    assert.equal(url, 'about:blank'); assert.equal(target, '_blank');
    const popup = { closed: false, opener: {}, close() { this.closed = true; },
      location: { replace(value) { navigations.push(value); } } };
    opened.push(popup); return popup;
  };
  return { opened, navigations };
}
for (const presentation of ['main', 'widget']) {
  for (const phase of ['pending', 'unavailable', 'foreign parent']) {
    test(`P5 independent external descriptor gate: ${presentation} ${phase}`, async () => {
      const f = fixture(), gate = deferred(), popup = independentP5PopupPort(f);
      const readNativeVoice = phase === 'pending' ? () => gate.promise
        : phase === 'unavailable' ? async () => { throw failure('native_voice_descriptor_unavailable'); }
        : async () => ({ client: voiceClientFixture(f), mount: voiceMountFixture({ parentOrigin: 'https://foreign.example' }) });
      const v = voiceStageFixture(f, { presentation, readNativeVoice });
      try {
        if (phase === 'pending') await turn(); else await v.stage.ready;
        assert.equal(v.launches.length, 0); assert.equal(v.takes(), 0);
        independentP5Control(v.host, 'external').emit('click'); await turn(); await turn();
        assert.equal(v.launches.length, 0, 'external cannot issue a signed launch before a successful descriptor and parent gate');
        assert.equal(popup.navigations.length, 0); assert.equal(v.takes(), 0);
        if (phase !== 'pending') assert.ok(popup.opened.every(value => value.closed), 'a denied lookup closes any owned blank popup');
      } finally {
        // Pending has no authorized destination; dispose before releasing the late result.
        v.stage.dispose(); gate.resolve(null); await v.stage.ready; await turn();
      }
    });
  }
  for (const transition of ['screen', 'account ABA', 'dispose']) {
    test(`P5 independent external descriptor await: ${presentation} ${transition}`, async () => {
      const f = fixture(), gate = deferred(), popup = independentP5PopupPort(f);
      const v = voiceStageFixture(f, { presentation, readNativeVoice: () => gate.promise });
      try {
        await turn(); independentP5Control(v.host, 'external').emit('click');
        if (transition === 'screen') f.app.screenSequence++;
        else if (transition === 'account ABA') { f.app.transitionAccount('account-B'); f.app.transitionAccount('account-A'); }
        else v.stage.dispose();
        gate.resolve({ client: voiceClientFixture(f), mount: voiceMountFixture() });
        await v.stage.ready; await turn(); await turn();
        assert.equal(v.launches.length, 0, 'external rechecks its original stage owner after metadata await');
        assert.equal(popup.navigations.length, 0); assert.equal(v.takes(), 0);
        assert.equal(f.nativeVoiceBridges.length, 0); assert.equal(v.host.querySelector('iframe'), null);
        assert.ok(popup.opened.every(value => value.closed), 'a stale continuation closes only its owned blank popup');
      } finally { v.stage.dispose(); gate.resolve(null); await v.stage.ready; }
    });
  }
  test(`P5 independent external descriptor positive: ${presentation} ready remains usable`, async () => {
    const f = fixture(), popup = independentP5PopupPort(f);
    const v = voiceStageFixture(f, { presentation, readNativeVoice: async () => ({ client: voiceClientFixture(f), mount: voiceMountFixture() }) });
    try {
      await v.stage.ready; const frame = v.host.querySelector('iframe'); assert.ok(frame); assert.equal(v.launches.length, 1);
      independentP5Control(v.host, 'external').emit('click');
      await voiceUntil(() => popup.navigations.length === 1);
      assert.equal(v.launches.length, 2); assert.equal(v.takes(), 0); assert.equal(popup.opened.length, 1);
      assert.equal(popup.opened[0].opener, null); assert.equal(popup.opened[0].closed, false);
      assert.equal(v.host.querySelector('iframe'), frame, 'external launch does not replace the running iframe');
      assert.ok(v.launches.every(value => value.expectedAccountId === 'account-A'));
      assert.equal(f.nativeVoiceBridges.length, 1, 'external continuation does not duplicate the stage bridge');
    } finally { v.stage.dispose(); }
  });
}


function p5OwnedPopupPort(f) {
  const opened = [], navigations = []; let foreignClosed = false;
  f.view.close = () => { foreignClosed = true; };
  f.view.open = (url, target) => {
    assert.equal(url, 'about:blank'); assert.equal(target, '_blank');
    const popup = { closed: false, opener: {}, document: { title: '', body: { textContent: '' } },
      close() { this.closed = true; }, location: { replace(value) { navigations.push(value); } } };
    opened.push(popup); return popup;
  };
  return { opened, navigations, foreignClosed: () => foreignClosed };
}
for (const presentation of ['main', 'widget']) {
  test(`P5 owned external popup preserves gesture and one shared metadata flight: ${presentation}`, async () => {
    const f = fixture(), gate = deferred(), popup = p5OwnedPopupPort(f); let reads = 0;
    const v = voiceStageFixture(f, { presentation, readNativeVoice: () => { reads++; return gate.promise; } });
    try {
      await turn(); independentP5Control(v.host, 'external').emit('click');
      independentP5Control(v.host, 'external').emit('click');
      assert.equal(popup.opened.length, 1, 'the user gesture synchronously owns one blank popup');
      assert.equal(popup.opened[0].opener, null); assert.equal(popup.opened[0].document.title, 'Открываем приложение');
      assert.equal(v.launches.length, 0); assert.equal(v.takes(), 0); assert.equal(popup.navigations.length, 0); assert.equal(reads, 1);
      gate.resolve({ client: voiceClientFixture(f), mount: voiceMountFixture() }); await v.stage.ready;
      await voiceUntil(() => popup.navigations.length === 1);
      assert.equal(reads, 1); assert.equal(v.launches.length, 2, 'runtime and the separate external entry retain their own existing admissions');
      assert.equal(popup.opened.length, 1); assert.equal(popup.opened[0].closed, false); assert.equal(popup.foreignClosed(), false);
      assert.equal(f.nativeVoiceBridges.length, 1); assert.ok(v.host.querySelector('iframe')); assert.equal(v.takes(), 0);
    } finally { v.stage.dispose(); gate.resolve(null); await v.stage.ready; }
  });
  test(`P5 explicit external retry shares bounded descriptor IO after unavailable: ${presentation}`, async () => {
    const f = fixture(), gate = deferred(), popup = p5OwnedPopupPort(f); let fetches = 0;
    const descriptor = createNativeVoiceDescriptor({ fetch: async () => { fetches++; if (fetches === 1) throw Error('synthetic metadata failure'); return gate.promise; } });
    const readNativeVoice = async options => { const mount = await descriptor.read(options); return mount ? { client: voiceClientFixture(f), mount } : null; };
    const v = voiceStageFixture(f, { presentation, readNativeVoice });
    try {
      await v.stage.ready; assert.equal(descriptor.snapshot().state, 'unavailable'); assert.equal(fetches, 1); assert.equal(v.launches.length, 0);
      independentP5Control(v.host, 'external').emit('click');
      assert.equal(popup.opened.length, 1); assert.equal(fetches, 2); assert.equal(v.launches.length, 0); assert.equal(v.takes(), 0);
      gate.resolve({ ok: true, async json() { return { nativeVoice: voiceMountFixture() }; } });
      await voiceUntil(() => popup.navigations.length === 1);
      assert.equal(fetches, 2); assert.equal(v.launches.length, 1); assert.equal(popup.opened.length, 1);
      assert.equal(popup.opened[0].closed, false); assert.equal(popup.foreignClosed(), false); assert.equal(f.nativeVoiceBridges.length, 1); assert.equal(v.takes(), 0);
    } finally { v.stage.dispose(); descriptor.close(); gate.resolve({ ok: false }); await v.stage.ready; }
  });
  test(`P5 blocked external popup does not retry unavailable metadata or issue admission: ${presentation}`, async () => {
    const f = fixture(); let opens = 0, reads = 0;
    f.view.open = (url, target) => { assert.equal(url, 'about:blank'); assert.equal(target, '_blank'); opens++; return null; };
    const v = voiceStageFixture(f, { presentation, readNativeVoice: async () => { reads++; throw failure('native_voice_descriptor_unavailable'); } });
    try {
      await v.stage.ready; independentP5Control(v.host, 'external').emit('click'); await turn();
      assert.equal(opens, 1); assert.equal(reads, 1); assert.equal(v.launches.length, 0); assert.equal(v.takes(), 0);
      assert.equal(v.host.querySelector('iframe'), null);
    } finally { v.stage.dispose(); }
  });
}


// The real header control, save guard, route writer and screen disposal run here.
// The field renderer and note I/O are controlled ports; this is not pixel QA.
function fieldReturnFixture(view = 'notes') {
  const f = fixture({ hash: view === 'notes' ? '#notes/current-note' : `#${view}` });
  const pending = deferred(), mounts = [];
  let dirty = true, flushes = 0, disposals = 0, focuses = 0;
  const draft = { hasUnsavedChanges: () => dirty, flush() { flushes++; return pending.promise; }, dispose() { disposals++; } };
  Object.assign(f.app, {
    view, preferences: { view }, notesHandle: draft, unifiedField: null,
    unifiedFieldFilters: { mine: 'all', world: 'all' }, homeQuery: '', unifiedFieldFilter: 'all',
    sidebar: new ElementPort(), header: new ElementPort(), mobileNav: new ElementPort(), fieldNavigation: new ElementPort(),
    renderNavigation: f.Controller.prototype.renderNavigation, updateNavUnread() {}, persist() {}, loadPersonal() {},
    renderCurrent() { this.cleanScreen(); mounts.push(this.view); },
  });
  f.app.main.focus = () => { focuses++; };
  f.app.renderNavigation();
  assert.equal(f.app.fieldNavigation.hidden, false);
  assert.equal(f.app.fieldNavigation.dataset.fieldReturn, 'true');
  const control = f.app.fieldNavigation.children.find(node => node.dataset.navView === 'mine');
  assert.equal(f.app.fieldNavigation.children.length, 4);
  assert.equal(control.textContent, 'Моё'); assert.equal(control.getAttribute('aria-label'), 'Моё');
  assert.equal(control.getAttribute('aria-current'), 'false');
  assert.equal(control.getAttribute('aria-pressed'), null, 'a destination is not a field-mode toggle');
  return { ...f, pending, draft, mounts, control,
    click() { control.emit('click'); }, setDurable() { dirty = false; },
    get flushes() { return flushes; }, get disposals() { return disposals; }, get focuses() { return focuses; },
  };
}
for (const view of ['notes', 'messages', 'assistant', 'access', 'library']) {
  test(`one-click field return: ${view} reaches the real field route and focuses its content`, () => {
    const f = fieldReturnFixture(view); f.setDurable(); f.click();
    assert.equal(f.location.hash, '#mine'); assert.equal(f.app.activeRoute, '#mine');
    assert.equal(f.app.view, 'mine'); assert.deepEqual(f.mounts, ['mine']); assert.equal(f.disposals, 1);
    assert.equal(f.focuses, 1); assert.equal(f.flushes, 0);
    assert.equal(f.app.fieldNavigation.dataset.fieldReturn, 'false');
    assert.deepEqual(f.app.fieldNavigation.children.map(control => control.textContent), ['Моё', 'Сообщения', 'Поиск', 'Профиль']);
  });
}
test('one-click field return: a pending Notes flush holds the editor and repeated activation navigates once', async () => {
  const f = fieldReturnFixture(), sequence = f.app.screenSequence;
  f.click(); f.click(); assert.equal(f.flushes, 1); assert.equal(f.app.noteActionPending, true);
  assert.equal(f.location.hash, '#notes/current-note'); assert.equal(f.app.screenSequence, sequence);
  assert.equal(f.app.notesHandle, f.draft); assert.equal(f.disposals, 0); assert.equal(f.focuses, 0);
  f.setDurable(); f.pending.resolve(); await turn();
  assert.equal(f.location.hash, '#mine'); assert.deepEqual(f.mounts, ['mine']);
  assert.equal(f.app.screenSequence, sequence + 1); assert.equal(f.disposals, 1); assert.equal(f.focuses, 1);
});
for (const outcome of ['rejected', 'still-undurable']) {
  test(`one-click field return: ${outcome} Notes flush retains its route, draft and focus`, async () => {
    const f = fieldReturnFixture(), sequence = f.app.screenSequence;
    f.click();
    if (outcome === 'rejected') f.pending.reject(failure('notes_local_unavailable')); else f.pending.resolve();
    await turn(); assert.equal(f.flushes, 1); assert.equal(f.location.hash, '#notes/current-note');
    assert.equal(f.app.screenSequence, sequence); assert.equal(f.app.notesHandle, f.draft);
    assert.equal(f.disposals, 0); assert.equal(f.focuses, 0); assert.deepEqual(f.mounts, []);
    assert.equal(f.app.noteActionPending, false); assert.match(f.app.lastToast, /сохран/u);
  });
}
test('one-click field return: a held exit cannot navigate or steal focus after an account ABA', async () => {
  const f = fieldReturnFixture(); f.click(); assert.equal(f.flushes, 1);
  f.app.transitionAccount('account-B'); f.app.transitionAccount('account-A');
  const sequence = f.app.screenSequence, route = f.location.hash;
  f.setDurable(); f.pending.resolve(); await turn();
  assert.equal(f.app.screenSequence, sequence); assert.equal(f.location.hash, route);
  assert.equal(f.app.notesHandle, null); assert.equal(f.disposals, 1);
  assert.equal(f.focuses, 0); assert.deepEqual(f.mounts, []);
});

for (const scenario of ['initial accepted send', 'later history poll', 'newer text and reply', 'another tab already acknowledged', 'cleanup denied']) {
  test(`chat history reconciliation: ${scenario} clears only the accepted intent and retains focus`, async () => {
    const f = fixture(), disk = new Map(), page = deferred(); let denyRemoval = false, pollResponse;
    const storage = { getItem: key => disk.get(key) ?? null, setItem: (key, value) => disk.set(key, value), removeItem(key) { if (denyRemoval) throw Error('Synthetic cleanup denied'); disk.delete(key); } };
    const drafts = chatState.createChatDraftStore(storage, () => 'intent_original');
    drafts.beginSend('account-A', 'room_fixture', 'Original A', 'reply_original');
    Object.assign(f.ports['./dom'], { timeLabel: () => '12:00', avatar: () => f.makeElement('span') });
    f.app.chatDrafts = drafts; f.app.preferences = { motion: false };
    const target = { kind: 'direct', id: 'room_fixture', draftKey: 'room_fixture', name: 'Synthetic conversation', subtitle: 'Private', avatarContext: undefined,
      record: { relationshipId: 'relationship_fixture' },
      canWrite: true, canModerate: false, preferences: { pinned: false, muted: false }, async refresh() {},
      async request(action) { if (action === 'list') return pollResponse ?? page.promise; throw Error(`Unexpected chat action: ${action}`); }, setUnread() {} };
    const parent = f.makeElement('div');
    f.app.mountConversation(parent, target, { showHeader: false, notify() {} });
    const input = parent.querySelector('textarea'), composer = parent.querySelector('.sw-composer'), send = composer.querySelector('.sw-button-primary');
    input.focus(); assert.equal(input.value, 'Original A');
    if (scenario === 'newer text and reply') drafts.edit('account-A', 'room_fixture', 'New draft B', 'reply_next');
    if (scenario === 'another tab already acknowledged') chatState.createChatDraftStore(storage).acknowledge('account-A', 'room_fixture', 'intent_original');
    if (scenario === 'cleanup denied') denyRemoval = true;
    const accepted = { messages: [{ messageId: 'message_original', clientId: 'intent_original', seq: 1, text: 'Original A', author: { profileId: 'account-A', displayName: 'Alice', revision: 1 }, createdAt: 1800000000000, replyTo: null, removed: false }], hasMore: false };
    if (scenario === 'later history poll') { page.resolve({ messages: [], hasMore: false }); await turn(); assert.equal(input.value, 'Original A'); pollResponse = accepted; [...f.intervals.values()][0].fn(); }
    else page.resolve(accepted);
    await turn();
    assert.equal(drafts.read('account-A', 'room_fixture').pending, null);
    assert.equal(input.value, scenario === 'newer text and reply' ? 'New draft B' : '', 'The visible composer must agree with the reconciled durable/current draft');
    assert.equal(send.disabled, scenario !== 'newer text and reply');
    assert.equal(f.view.document.activeElement, input, 'A history acknowledgement must never move focus');
    if (scenario === 'newer text and reply') assert.equal(drafts.read('account-A', 'room_fixture').replyTo, 'reply_next');
    if (scenario === 'cleanup denied') {
      assert.equal(drafts.hasVolatile(), true); assert.equal(parent.querySelector('.sw-chat-draft-status').hidden, false);
      assert.match(parent.querySelector('.sw-chat-draft-status').textContent, /Не удалось очистить/u);
      assert.equal(chatState.createChatDraftStore(storage).read('account-A', 'room_fixture').pending.clientId, 'intent_original', 'Failed cleanup must not claim disk changed');
    }
    f.app.chatCleanup();
  });
}

for (const coarse of [false, true]) test(`chat composer keyboard: ${coarse ? 'touch Enter stays a newline; modified Enter sends' : 'desktop Enter sends'} without submitting composition`, async () => {
  const f = fixture({ coarse }), disk = new Map();
  f.app.chatDrafts = chatState.createChatDraftStore({ getItem: key => disk.get(key) ?? null, setItem: (key, value) => disk.set(key, value), removeItem: key => disk.delete(key) });
  f.app.chatDrafts.edit('account-A', 'keyboard_room', 'Keyboard draft');
  f.app.preferences = { motion: false };
  const parent = f.makeElement('div'), target = { kind: 'community', id: 'keyboard_room', draftKey: 'keyboard_room', name: 'Keyboard fixture', record: {},
    canWrite: true, canModerate: false, preferences: { pinned: false, muted: false }, async refresh() {}, async request() { return { messages: [], hasMore: false }; }, setUnread() {} };
  f.app.mountConversation(parent, target, { showHeader: false, notify() {} });
  const input = parent.querySelector('textarea'), composer = parent.querySelector('.sw-composer');
  let submissions = 0, prevented = 0; composer.requestSubmit = () => { submissions++; };
  const key = modifiers => input.emit('keydown', { key: 'Enter', keyCode: 13, ctrlKey: false, metaKey: false, shiftKey: false, altKey: false, isComposing: false,
    preventDefault() { prevented++; }, ...modifiers });
  key({ isComposing: true }); key({ keyCode: 229 }); key({ shiftKey: true });
  assert.equal(submissions, 0); assert.equal(prevented, 0);
  key({}); assert.equal(submissions, coarse ? 0 : 1); assert.equal(prevented, coarse ? 0 : 1);
  if (coarse) { key({ ctrlKey: true }); key({ metaKey: true }); assert.equal(submissions, 2); assert.equal(prevented, 2); }
  assert.equal(input.enterKeyHint, coarse ? 'enter' : 'send');
  await turn(); f.app.chatCleanup();
});
