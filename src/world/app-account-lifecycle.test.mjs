import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';
import * as appLaunch from './app-launch.mjs';
import * as appAudience from './app-audience.mjs';
import * as appActions from './app-actions.mjs';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';
import { createConnectService } from '../../modules/connect/server/index.mjs';
import { validateState } from '../../modules/connect/browser/storage.mjs';

// Exercise the actual controller methods. UI/network ports are inert fixtures;
// no source regex assertions, copied transition implementation or DOM package.
const source = await readFile(new URL('./app.ts', import.meta.url), 'utf8');
const compiled = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const stageSource = await readFile(new URL('./app-stage.ts', import.meta.url), 'utf8');
const stageCompiled = ts.transpileModule(stageSource, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };
const turn = () => new Promise(resolve => setImmediate(resolve));
const failure = code => Object.assign(new Error(code), { code });
class ElementPort {
  children = []; open = true; isConnected = true; textContent = ''; changes = 0; dataset = {}; tagName = ''; className = '';
  classList = { add() {}, remove() {}, toggle() {} };
  append(...nodes) { this.children.push(...nodes); this.changes++; }
  prepend(...nodes) { this.children.unshift(...nodes); this.changes++; }
  insertBefore(node, target) { const index = this.children.indexOf(target); if (index < 0) this.append(node); else this.children.splice(index, 0, node); }
  replaceChildren(...nodes) { this.children = nodes; this.changes++; }
  querySelector(selector) {
    for (const child of this.children) {
      if (!(child instanceof ElementPort)) continue;
      if (selector.startsWith('.') ? child.className.split(' ').includes(selector.slice(1)) : child.tagName === selector) return child;
      const found = child.querySelector(selector); if (found) return found;
    }
    return null;
  }
  addEventListener() {}
  setAttribute() {}
  removeAttribute() {}
  contains(node) { return node === this || this.children.some(child => child instanceof ElementPort && child.contains(node)); }
  focus() {}
  remove() { this.isConnected = false; }
}
function fixture({ initial = 'account-A', hash = '#mine' } = {}) {
  const module = { exports: {} }, location = { hash, href: `https://shell.example/${hash}` };
  const document = { activeElement: null, visibilityState: 'visible', addEventListener() {} };
  const view = Object.assign(new EventTarget(), { location, Node: ElementPort, navigator: { userActivation: { isActive: false } }, matchMedia: () => ({ matches: false, addEventListener() {} }) });
  document.defaultView = view;
  const makeElement = (tagName, className, label) => Object.assign(new ElementPort(), { ownerDocument: document, textContent: label ?? '', tagName, className: className ?? '' });
  const ports = {
    './dom': { el: makeElement, button: label => makeElement('button', '', label), iconButton: label => makeElement('button', '', label), emptyState: label => makeElement('div', '', label) },
    './dialogs': { errorText: error => error.code ?? 'failure' },
    './product': { loadDeskPreferences: accountId => ({ favorites: [`favorite-${accountId}`], recent: [], pinnedApps: [`pin-${accountId}`] }) },
    './hex-field': { createHexFieldState: () => ({ fresh: true }) },
    './app-launch.mjs': appLaunch,
    './app-audience.mjs': appAudience,
    './app-actions.mjs': appActions,
    './application-card': { appTone: () => 'neutral' },
    './app-saved': { mountAppSaved: () => ({ dispose() {}, async refresh() {} }) },
    './app-discussion': { mountAppDiscussion: () => ({ dispose() {}, async refresh() {}, async flush() {}, hasUnsavedChanges: () => false, setVisible() {}, async updateEntry() {}, async updateSelection() {}, focus() {} }) },
  };
  const stageModule = { exports: {} };
  vm.runInNewContext(stageCompiled, { module: stageModule, exports: stageModule.exports, require: name => ports[name] ?? {}, AbortController });
  ports['./app-stage'] = stageModule.exports;
  vm.runInNewContext(`${compiled}\nexports.TestController = WorldApplication;`, {
    module, exports: module.exports, require: name => ports[name] ?? {}, location,
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
    desk: { favorites: [`favorite-${initial}`], recent: [] }, fieldState: {}, discoveryScope: 'private-scope', discoveryPages: ['private-cursor'],
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
  return { app, Controller, location, requests, personalFrames, dialogs, setLocal(value) { local = value; }, setOnline(value) { online = value; }, setApiError(value) { apiError = value; } };
}

test('actual refresh A → offline B → online B clears every private cache before B metadata is available', async () => {
  const f = fixture(), oldDialog = f.app.dialog();
  let disposed = 0; f.app.notesHandle = { dispose() { disposed++; } };
  f.setLocal('account-B'); f.setOnline(false); await f.app.refresh(true);
  assert.equal(f.app.deskAccount, 'account-B'); assert.equal(f.app.profile, null);
  for (const key of ['apps', 'communities', 'devices']) assert.equal(f.app[key].length, 0, key);
  assert.equal(f.app.homeNotes, null); assert.equal(f.app.results.people.length, 0);
  assert.equal(f.app.homeState.slots.size, 0); assert.equal(f.app.query, '');
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

test('actual app controller enqueues launch before slow optional metadata on the real serialized Connect client', async t => {
  for (const held of ['apps.list', 'world.community.get']) {
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
      operations: new Set(['apps.launch', 'apps.list', 'world.community.get']), execute({ op, actor }) {
        operations.push(op);
        if (op === 'apps.launch') return { launchUrl, entry: { appId, domainId, origin: 'https://app.runtime.example', path: '/' } };
        if (op === 'world.community.get') return { community: { communityId: 'community-test', membership: { state: 'none' } } };
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
    const route = appLaunch.formatAppLaunchRoute(held === 'apps.list' ? { appId, domainId } : { appId }, held === 'apps.list' ? undefined : 'community-test');
    const f = fixture({ initial: identity.accountId, hash: `#${route}` }); f.app.communities = [];
    f.app.api = { request: (op, args) => client.extension(op, JSON.parse(JSON.stringify(args ?? {})), { expectedAccountId: identity.accountId }) };
    const opening = f.app.openApplication({ appId, name: 'Приложение' }, appLaunch.parseAppLaunchRoute(route), true);
    try {
      await entered.promise; await turn();
      assert.equal(operations[0], 'apps.launch', held);
      assert.equal(f.app.main.querySelector('iframe')?.src, launchUrl, 'runtime frame is mounted while optional response is still held');
      await opening;
    } finally { release.resolve(); await opening; }
    await client.getLocalState(); await turn();
    assert.equal(f.app.main.querySelector('iframe')?.title, 'Actual app name');
    f.app.cleanScreen();
  }
});
