import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import WebSocket from 'ws';
import { createAppsService } from '../../modules/apps/server/index.mjs';
import { createWorldService } from '../../modules/world/server/index.mjs';
import { createAppLauncher } from './app-launch.mjs';
import { createAppSavedState, dispatchAppSavedIntent, createAppSavedLibraryState } from './app-saved-state.mjs';
import { createAppDiscussionDraftState, dispatchAppDiscussionIntent, listAppDiscussionDrafts, createAppDiscussionFeed } from './app-discussion-state.mjs';

// Independent deferred client boundaries. Real HTTP/ACL entry admission is
// covered separately by server/test/app-entry-http.test.mjs. These adapters do
// not stand in for a real browser's popup policy, iframe or Web Locks behavior.
const appId = `app-${'81'.repeat(16)}`, domainId = `dom_${'82'.repeat(16)}`;
const accountId = 'independent-engagement-account', shellUrl = 'https://shell.example';
const entry = (path = '/original?tag=a%2Bb#screen') => ({ appId, domainId, origin: 'https://app.other.example', path });
const clone = value => structuredClone(value);
function deferred() { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; }
function boot(value, marker = 'a') { return `${value.origin}/_soty/boot?${new URLSearchParams({ path: value.path })}#${marker.repeat(43)}`; }
function popup() {
  const state = { closed: false, destinations: [], opener: {} };
  return { state, handle: { get closed() { return state.closed; }, get opener() { return state.opener; }, set opener(value) { state.opener = value; },
    location: { replace(value) { state.destinations.push(value); } }, close() { state.closed = true; } } };
}
const pause = ms => new Promise(done => setTimeout(done, ms));
const code = expected => error => { assert.equal(error?.code, expected); return true; };
const owner = { accountId: 'engagement_owner', deviceId: 'owner_browser', label: 'Owner' };
const reader = { accountId: 'engagement_reader', deviceId: 'reader_browser', label: 'Reader' };
async function until(check, label) {
  const deadline = Date.now() + 5000;
  do { if (check()) return; await pause(5); } while (Date.now() < deadline);
  assert.fail(`Timed out: ${label}`);
}
function localAdapters(t) {
  const values = new Map(), queues = new Map(), handles = []; let failure = false, serial = 0, nextGate = null;
  const storage = { getItem: key => values.get(key) ?? null, setItem(key, value) { if (failure) throw new Error('synthetic quota failure'); values.set(key, value); } };
  const locks = { request(key, callback) {
    const previous = queues.get(key) ?? Promise.resolve(), gate = nextGate; nextGate = null;
    const work = previous.catch(() => {}).then(async () => { if (gate) { gate.entered.resolve(); await gate.release.promise; } return callback(); });
    queues.set(key, work.catch(() => {})); return work;
  } };
  const randomId = () => `independent-client-${++serial}`;
  const track = value => { handles.push(value); return value; };
  t.after(() => { for (const value of handles) value.dispose(); });
  return { storage, locks, randomId, values, track, fail(value) { failure = value; },
    saved(account = reader.accountId) { return track(createAppSavedState({ accountId: account, storage, locks, randomId })); },
    draft(scope) { return track(createAppDiscussionDraftState({ scope, storage, locks, randomId })); },
    holdNext() { nextGate = { entered: deferred(), release: deferred() }; return nextGate; } };
}
async function model(t) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-engagement-client-independent-'));
  const world = createWorldService({ databasePath: join(directory, 'world.sqlite'), projectId: 'engagement_ui_acceptance' });
  const sockets = new Set(); let service, clock = 1_800_000_000_000, serial = 0;
  const gateway = createServer((_req, res) => res.writeHead(404).end());
  gateway.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  gateway.on('upgrade', (req, socket, head) => { if (!service?.handleUpgrade(req, socket, head)) socket.destroy(); });
  await new Promise(done => gateway.listen(0, '127.0.0.1', done));
  const port = gateway.address().port, token = randomBytes(32).toString('base64url');
  const options = { databasePath: join(directory, 'apps.sqlite'), now: () => clock,
    appOriginTemplate: `http://{appId}.legacy.localhost:${port}`, namedAppZone: `http://named.localhost:${port}`, shellOrigins: [`http://localhost:${port}`],
    actorActive: value => [owner, reader].some(actor => actor.accountId === value?.accountId && actor.deviceId === value.deviceId),
    authenticateConnector: async value => value.token === token,
    canAccessCommunity: (account, group) => world.canAccessCommunity(account, group), isGroupAdmin: (account, group) => world.isGroupAdmin(account, group),
    withAuthorityFence: callback => world.withCommunityAuthorityFence(callback),
    readCommunityAuthority: (actor, account, ids) => world.appCommunityAuthority(actor.accountId, account, ids) };
  service = createAppsService(options);
  t.after(async () => {
    service.close(); world.close(); for (const socket of sockets) socket.destroy(); await new Promise(done => gateway.close(done));
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(basename(directory), /^soty-engagement-client-independent-/u);
    await rm(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
  });
  const call = (op, args, actor = owner) => { clock += 2500; return service.execute({ op, args: { expectedAccountId: actor.accountId, ...args }, actor }); };
  const identity = { linkId: 'engagement_client_link', hostDeviceId: 'engagement_client_host', connectorId: 'engagement_client_connector' };
  const claimCode = randomBytes(32).toString('base64url'), frames = [];
  const ws = new WebSocket(`ws://127.0.0.1:${port}/api/apps/channel`); ws.on('message', data => frames.push(JSON.parse(data.toString()))); ws.on('error', () => {});
  await new Promise((done, reject) => { ws.once('open', done); ws.once('error', reject); });
  ws.send(JSON.stringify({ type: 'auth', schema: 'soty.apps-channel.v1', ...identity, token, name: 'Never public source device' }));
  await until(() => frames.some(value => value.type === 'ready'), 'claim transport');
  ws.send(JSON.stringify({ type: 'claim', claimDigest: createHash('sha256').update(claimCode).digest('hex') }));
  await until(() => frames.some(value => value.type === 'claim-ready'), 'claim digest');
  call('apps.claim', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, claimCode });
  const closed = new Promise(done => ws.once('close', done)); ws.close(); await closed;
  const make = () => call('apps.register', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, port: 18000 + ++serial,
    name: `Independent discussion ${serial}`, entryPath: '/start', grants: { accountIds: [reader.accountId] } }).app;
  const app = make();
  const selected = call('apps.entry.get', { appId: app.id, path: '/board?x=a%2Bb#view' }, reader).entry;
  const api = { async request(op, args) { return call(op, args, reader); } };
  const context = () => call('apps.discussion.context', { appId: app.id, domainId: selected.domainId, path: selected.path }, reader);
  return { app, selected, call, api, make, context,
    scope(conversationId = context().context.conversationId) { return { accountId: reader.accountId, appId: app.id, conversationId, entry: clone(selected) }; },
    saved() { return call('apps.saved.get', { appId: app.id }, reader); },
    rotate() { call('apps.update', { appId: app.id, grants: { accountIds: [reader.accountId, `new-reader-${++serial}`] } }); },
    revoke() { call('apps.revoke', { appId: app.id }); },
    reopen() { service.close(); service = createAppsService(options); },
    send(body, conversationId = context().context.conversationId) { return call('apps.discussion.send', { appId: app.id, domainId: selected.domainId,
      path: selected.path, conversationId, body, requestId: `independent-server-${++serial}` }, reader); } };
}

test('D3 initial admission and immediate external gesture share one resolved entry but receive distinct tickets', async () => {
  const first = deferred(), calls = [], window = popup(); let defaultPath = '/first?x=%2B#view', serial = 0;
  const launcher = createAppLauncher({ accountId, target: { appId }, shellUrl, isCurrent: () => true,
    async request(args) {
      calls.push(clone(args)); if (++serial === 1) return first.promise;
      const resolved = entry(args.path ?? defaultPath); return { url: boot(resolved, 'b'), entry: resolved };
    } });
  const initial = launcher.launch(), outside = launcher.openExternal(() => window.handle);
  assert.equal(window.state.opener, null); assert.equal(calls.length, 1, 'external gesture must not resolve an independent default');
  const resolved = entry(defaultPath); defaultPath = '/different-default';
  first.resolve({ url: boot(resolved), entry: resolved });
  assert.equal(await initial, boot(resolved)); assert.equal(await outside, 'opened');
  assert.deepEqual(calls[1], { appId, domainId, path: resolved.path, expectedAccountId: accountId });
  assert.deepEqual(launcher.entry(), resolved); assert.deepEqual(window.state.destinations, [boot(resolved, 'b')]);
  assert.notEqual(window.state.destinations[0], boot(resolved)); launcher.dispose();
  assert.equal(window.state.closed, false, 'completed tabs are owned by the person, not later panel disposal');
});

test('D3 failed admission resolves one offline entry before a queued external retry and never substitutes a later default', async () => {
  const lookup = deferred(), requests = [], resolutions = [], window = popup(); let first = true;
  const launcher = createAppLauncher({ accountId, target: { appId }, shellUrl, isCurrent: () => true,
    async request(args) {
      requests.push(clone(args)); if (first) { first = false; throw Object.assign(new Error('offline'), { code: 'app_offline' }); }
      const resolved = entry(args.path ?? '/UNRELATED'); return { url: boot(resolved, 'c'), entry: resolved };
    }, async resolveEntry(args) { resolutions.push(clone(args)); return lookup.promise; } });
  const initial = launcher.launch().then(value => ({ value }), error => ({ error }));
  const outside = launcher.openExternal(() => window.handle);
  await Promise.resolve(); await Promise.resolve();
  assert.equal(requests.length, 1); assert.equal(resolutions.length, 1); assert.deepEqual(window.state.destinations, []);
  const resolved = entry('/read-without-runtime#/original'); lookup.resolve({ entry: resolved });
  assert.equal((await initial).error.code, 'app_offline'); assert.equal(await outside, 'opened');
  assert.deepEqual(requests[1], { appId, domainId, path: resolved.path, expectedAccountId: accountId });
  assert.deepEqual(launcher.entry(), resolved); launcher.dispose();
});

test('D3 a successful launch with a missing or mismatched entry cannot be guessed by a later metadata lookup', async () => {
  const base = entry();
  for (const value of [undefined, { ...base, appId: `app-${'83'.repeat(16)}` }, { ...base, domainId: `dom_${'84'.repeat(16)}` },
    { ...base, path: '/different' }, { ...base, origin: 'https://another.other.example' }, { ...base, title: 'unexpected private metadata' }]) {
    let lookups = 0, requests = 0; const answer = deferred(), window = popup();
    const launcher = createAppLauncher({ accountId, target: { appId, domainId, path: base.path }, shellUrl, isCurrent: () => true,
      async request() { requests++; return answer.promise; }, async resolveEntry() { lookups++; return { entry: base }; } });
    const initial = launcher.launch().then(value => ({ value }), error => ({ error }));
    const outside = launcher.openExternal(() => window.handle).then(value => ({ value }), error => ({ error }));
    answer.resolve({ url: boot(base), entry: value });
    assert.equal((await initial).error.code, 'invalid_app_entry'); assert.equal((await outside).error.code, 'invalid_app_entry');
    assert.equal(requests, 1); assert.equal(lookups, 0); assert.equal(launcher.entry(), null);
    assert.equal(window.state.closed, true); assert.deepEqual(window.state.destinations, []); launcher.dispose();
  }
});

test('D3 an account A-to-B-to-A generation change cannot apply an old launch or redirect its pending popup', async () => {
  let currentAccount = accountId, generation = 1; const expectedGeneration = generation, answer = deferred(), window = popup();
  const launcher = createAppLauncher({ accountId, target: { appId }, shellUrl,
    isCurrent: expected => currentAccount === expected && generation === expectedGeneration,
    async request() { return answer.promise; } });
  const initial = launcher.launch(), outside = launcher.openExternal(() => window.handle);
  currentAccount = 'other-account'; generation++; currentAccount = accountId; generation++;
  answer.resolve({ url: boot(entry()), entry: entry() });
  assert.equal(await initial, null); assert.equal(await outside, 'stale'); assert.equal(launcher.entry(), null);
  assert.equal(window.state.closed, true); assert.deepEqual(window.state.destinations, []); launcher.dispose();
});

test('D3 saved lost ACK reload replays the exact intent while reporting a later removal as current', async t => {
  const f = await model(t), local = localAdapters(t), state = local.saved(); let firstArgs;
  await assert.rejects(dispatchAppSavedIntent({ state, intent: { entry: f.selected, saved: true, expectedRevision: 0, currentEntry: null },
    isCurrent: () => true, api: { async request(op, args) { firstArgs = clone(args); await f.api.request(op, args); throw new TypeError('response lost'); } } }));
  const pending = state.read().pending; assert.deepEqual(pending.args, firstArgs); state.dispose();
  f.call('apps.saved.set', { appId: f.app.id, saved: false, expectedRevision: 1, requestId: 'another-window-remove' }, reader);
  const restored = local.saved(), displayed = restored.read().pending, calls = [];
  const result = await dispatchAppSavedIntent({ state: restored, expectedPending: displayed, isCurrent: () => true,
    api: { async request(op, args) { calls.push(clone(args)); return f.api.request(op, args); } } });
  assert.equal(result.status, 'accepted'); assert.deepEqual(calls, [firstArgs]); assert.equal(result.response.replayed, true);
  assert.equal(result.response.receipt.saved, true); assert.equal(result.response.current.entry, null); assert.equal(result.response.current.revision, 2);
  assert.equal(restored.read().pending, null); assert.equal(f.saved().entry, null);
});

test('D3 replacing an account pending slot never dispatches the new app from an old retry or clears it with an old ACK', async t => {
  const f = await model(t), local = localAdapters(t), first = local.saved(), second = local.saved();
  const pendingA = await first.prepare({ entry: f.selected, saved: true, expectedRevision: 0, currentEntry: null });
  const admitted = deferred(), answer = deferred(); let requests = 0;
  const api = { async request(op, args) { requests++; const response = await f.api.request(op, args); admitted.resolve(); await answer.promise; return response; } };
  const inFlight = dispatchAppSavedIntent({ state: first, expectedPending: pendingA, api, isCurrent: () => true }); await admitted.promise;
  assert.equal(await second.abandon(pendingA), true);
  const otherApp = f.make(), otherEntry = f.call('apps.entry.get', { appId: otherApp.id }, reader).entry;
  const pendingB = await second.prepare({ entry: otherEntry, saved: true, expectedRevision: 1, currentEntry: null });
  await assert.rejects(dispatchAppSavedIntent({ state: first, expectedPending: pendingA, api, isCurrent: () => true }), code('app_saved_pending_changed'));
  await assert.rejects(dispatchAppSavedIntent({ state: first, api, isCurrent: () => true }), code('app_saved_pending_changed'));
  assert.equal(requests, 1); answer.resolve(); assert.equal((await inFlight).status, 'superseded');
  assert.deepEqual(second.read().pending, pendingB); assert.equal(f.saved().entry.appId, f.app.id);
  assert.equal(f.call('apps.saved.get', { appId: otherApp.id }, reader).entry, null);
});

test('D3 saved storage failure dispatches nothing and an explicit different-entry replacement remains required', async t => {
  const f = await model(t), local = localAdapters(t), state = local.saved(); let calls = 0;
  const api = { async request(op, args) { calls++; return f.api.request(op, args); } };
  const intent = { entry: f.selected, saved: true, expectedRevision: 0, currentEntry: null };
  local.fail(true); await assert.rejects(dispatchAppSavedIntent({ state, intent, api, isCurrent: () => true }), code('app_saved_storage_unavailable'));
  assert.equal(calls, 0); assert.equal(state.read().pending, null); local.fail(false);
  await dispatchAppSavedIntent({ state, intent, api, isCurrent: () => true });
  const current = f.saved(), different = { ...f.selected, path: '/different#view' };
  await assert.rejects(dispatchAppSavedIntent({ state, intent: { entry: different, saved: true, expectedRevision: current.revision, currentEntry: current.entry },
    api, isCurrent: () => true }), code('app_saved_replace_required'));
  assert.equal(calls, 1); assert.deepEqual(f.saved().entry.path, f.selected.path);
});

test('D3 late saved ACK may settle only its original account record and never produce an accepted visible result after account change', async t => {
  const f = await model(t), local = localAdapters(t), state = local.saved(), other = local.saved('different-account');
  const admitted = deferred(), answer = deferred(); let active = true;
  const task = dispatchAppSavedIntent({ state, intent: { entry: f.selected, saved: true, expectedRevision: 0, currentEntry: null }, isCurrent: () => active,
    api: { async request(op, args) { const value = await f.api.request(op, args); admitted.resolve(); await answer.promise; return value; } } });
  await admitted.promise; active = false; answer.resolve(); const result = await task;
  assert.equal(result.status, 'stale'); assert.equal(result.response, undefined); assert.equal(state.read().pending, null);
  assert.deepEqual(other.read(), { revision: 0, pending: null }); assert.equal(f.saved().revision, 1);
});

test('D3 old audience pending survives reload and exact replay without entering the new conversation or exposing lost-access body', async t => {
  const f = await model(t), local = localAdapters(t), scope = f.scope(), state = local.draft(scope);
  state.edit({ text: 'Text for the original audience only', replyTo: null }); assert.equal(await state.flush(), true);
  const clicked = state.read().draft;
  await assert.rejects(dispatchAppDiscussionIntent({ state, expectedDraft: clicked, beforeCreate: () => true, isCurrent: () => true,
    api: { async request(op, args) { await f.api.request(op, args); throw new TypeError('message ACK lost'); } } }));
  const pending = state.read().pending; state.dispose(); f.rotate();
  const newScope = f.scope(), current = local.draft(newScope); assert.notEqual(newScope.conversationId, scope.conversationId);
  assert.equal(current.read().draft.text, ''); assert.equal(current.read().pending, null);
  assert.equal(current.readRetained()[0].scope.conversationId, scope.conversationId);
  current.edit({ text: 'New audience draft stays separate', replyTo: null }); await current.flush();
  const restored = local.draft(scope); assert.deepEqual(restored.read().pending, pending);
  f.revoke(); const args = [];
  const result = await dispatchAppDiscussionIntent({ state: restored, expectedPending: pending, isCurrent: () => true,
    api: { async request(op, value) { args.push(clone(value)); return f.api.request(op, value); } } });
  assert.equal(result.status, 'accepted'); assert.equal(result.response.replayed, true); assert.equal(result.response.message, null);
  assert.deepEqual(args, [pending.args]); assert.equal(result.response.receipt.conversationId, scope.conversationId);
  assert.equal(current.read().draft.text, 'New audience draft stays separate'); assert.equal(current.read().pending, null);
  assert.equal(restored.read().draft.text, ''); assert.equal(restored.read().pending, null);
});

test('D3 message ACK for clicked text preserves text typed later and a wrong receipt leaves the exact pending intact', async t => {
  const f = await model(t), local = localAdapters(t), state = local.draft(f.scope()), admitted = deferred(), answer = deferred();
  state.edit({ text: 'First message', replyTo: null }); await state.flush(); const expected = state.read().draft;
  let response;
  const inFlight = dispatchAppDiscussionIntent({ state, expectedDraft: expected, beforeCreate: () => true, isCurrent: () => true,
    api: { async request(op, args) { response = await f.api.request(op, args); admitted.resolve(); await answer.promise; return response; } } });
  await admitted.promise; const pending = state.read().pending;
  state.edit({ text: 'Second unsent message', replyTo: null }); await state.flush();
  await assert.rejects(state.acknowledge(pending, { ...response, requestId: 'different-request' }), code('app_discussion_invalid_receipt'));
  assert.deepEqual(state.read().pending, pending); answer.resolve(); assert.equal((await inFlight).status, 'accepted');
  assert.equal(state.read().draft.text, 'Second unsent message'); assert.equal(state.read().pending, null);
  assert.deepEqual(f.context().messages.map(value => value.body), ['First message']);
  const reloaded = local.draft(state.scope); assert.equal(reloaded.read().draft.text, 'Second unsent message');
});

test('D3 two local writers preserve their different unsent texts and require explicit conflict resolution', async t => {
  const f = await model(t), local = localAdapters(t), scope = f.scope(), first = local.draft(scope), second = local.draft(scope);
  first.edit({ text: 'First window text', replyTo: null }); second.edit({ text: 'Second window text', replyTo: null });
  assert.equal(await first.flush(), true); assert.equal(await second.flush(), false);
  assert.equal(second.read().conflict, true); assert.equal(second.read().draft.text, 'Second window text');
  assert.equal(second.read().remoteDraft.text, 'First window text');
  let sends = 0;
  await assert.rejects(dispatchAppDiscussionIntent({ state: second, expectedDraft: second.read().draft, beforeCreate: () => true, isCurrent: () => true,
    api: { async request() { sends++; throw new Error('must not dispatch'); } } }), code('app_discussion_draft_conflict'));
  assert.equal(sends, 0); const remote = second.read().remoteDraft;
  await second.keepMine(remote.revision); assert.equal(first.read().draft.text, 'Second window text');
  assert.equal(first.read().conflict, false); assert.equal(f.context().messages.length, 0);
});

test('D3 failure to durably stage a message and a changed audience while waiting for the lock both dispatch zero requests', async t => {
  const f = await model(t), local = localAdapters(t), state = local.draft(f.scope()); let calls = 0, permitted = true;
  const api = { async request(op, args) { calls++; return f.api.request(op, args); } };
  state.edit({ text: 'Recoverable unsent text', replyTo: null }); const original = state.read().draft;
  assert.equal(state.hasUnsavedChanges(), true); local.fail(true);
  await assert.rejects(dispatchAppDiscussionIntent({ state, expectedDraft: original, beforeCreate: () => true, api, isCurrent: () => true }), code('app_discussion_storage_unavailable'));
  assert.equal(calls, 0); assert.equal(state.read().draft.text, original.text); assert.equal(state.read().pending, null);
  local.fail(false); assert.equal(await state.flush(), true);
  const gate = local.holdNext();
  const pending = dispatchAppDiscussionIntent({ state, expectedDraft: state.read().draft, beforeCreate: () => permitted, api, isCurrent: () => true });
  await gate.entered.promise; permitted = false; gate.release.resolve();
  await assert.rejects(pending, code('app_discussion_context_changed'));
  assert.equal(calls, 0); assert.equal(state.read().pending, null); assert.equal(state.read().draft.text, original.text);
  assert.equal(f.context().messages.length, 0);
});

test('D3 local draft recovery is scoped to the exact entry and account even when an alias is no longer available', async t => {
  const f = await model(t), local = localAdapters(t), scope = f.scope(), state = local.draft(scope);
  state.edit({ text: 'A locally retained draft, never a history grant', replyTo: null }); await state.flush(); f.revoke();
  const lookup = extra => listAppDiscussionDrafts({ accountId: reader.accountId, appId: scope.appId, domainId: scope.entry.domainId, path: scope.entry.path,
    origin: scope.entry.origin, storage: local.storage, ...extra });
  assert.equal(lookup({}).length, 1); assert.deepEqual(lookup({ path: '/another-path' }), []);
  assert.deepEqual(lookup({ domainId: `dom_${'85'.repeat(16)}` }), []); assert.deepEqual(lookup({ accountId: owner.accountId }), []);
  assert.deepEqual(lookup({ origin: 'https://different-origin.invalid' }), []);
  assert.throws(() => f.context(), code('app_unavailable'));
});

test('D3 saved pagination reset does not merge different library revisions or erase a durable pending intent', async t => {
  const f = await model(t), local = localAdapters(t), state = local.saved(), apps = [f.app];
  while (apps.length < 21) apps.push(f.make());
  for (let i = 0; i < apps.length; i++) f.call('apps.saved.set', { appId: apps[i].id, saved: true, expectedRevision: i,
    requestId: `paginate-save-${i}` }, reader);
  const library = local.track(createAppSavedLibraryState({ accountId: reader.accountId }));
  assert.equal(await library.load({ api: f.api, isCurrent: () => true }), 'accepted');
  assert.equal(library.read().entries.length, 20); assert.ok(library.read().nextCursor);
  const saved = f.saved(), pending = await state.prepare({ entry: f.selected, saved: false, expectedRevision: saved.revision, currentEntry: saved.entry });
  f.call('apps.saved.set', { appId: apps[5].id, saved: false, expectedRevision: 21, requestId: 'other-window-during-pagination' }, reader);
  assert.equal(await library.load({ api: f.api, isCurrent: () => true, older: true }), 'reset');
  assert.deepEqual(library.read().entries, []); assert.equal(library.read().resetRequired, true);
  assert.deepEqual(state.read().pending, pending);
  await library.load({ api: f.api, isCurrent: () => true }); assert.equal(library.read().entries.length, 20);
  assert.equal(library.read().entries.some(value => value.appId === apps[5].id), false); assert.deepEqual(state.read().pending, pending);
});

test('D3 native authentication_required clears previously loaded saved metadata before account observers catch up', async t => {
  const f = await model(t), local = localAdapters(t);
  f.call('apps.saved.set', { appId: f.app.id, saved: true, expectedRevision: 0, requestId: 'private-list-first' }, reader);
  const library = local.track(createAppSavedLibraryState({ accountId: reader.accountId }));
  await library.load({ api: f.api, isCurrent: () => true }); assert.equal(library.read().entries.length, 1);
  await assert.rejects(library.load({ api: { async request() { throw new TypeError('temporary network failure'); } }, isCurrent: () => true }));
  assert.equal(library.read().entries.length, 1, 'transport failure is not an affirmative account denial');
  assert.equal(library.read().stale, true);
  // Actual service actor changed; isCurrent intentionally models the short
  // interval before the shell's account observer invalidates its old view.
  const changedIdentity = { async request(op, args) { return f.call(op, args, owner); } };
  await assert.rejects(library.load({ api: changedIdentity, isCurrent: () => true }), code('authentication_required'));
  assert.deepEqual(library.read().entries, []); assert.equal(library.read().nextCursor, null);
});

test('D3 deletion outside the latest tail and server cursor restart reconcile the feed without changing draft or pending', async t => {
  const f = await model(t), local = localAdapters(t), scope = f.scope(), sent = [];
  for (let i = 0; i < 35; i++) sent.push(f.send(`Message ${i}`, scope.conversationId));
  const feed = local.track(createAppDiscussionFeed({ accountId: reader.accountId, appId: f.app.id, entry: f.selected }));
  await feed.load({ api: f.api, isCurrent: () => true }); assert.equal(feed.read().messages.length, 30);
  await feed.older({ api: f.api, isCurrent: () => true }); assert.equal(feed.read().messages.length, 35);
  const draft = local.draft(scope); draft.edit({ text: 'My local text survives cursor reset', replyTo: null }); await draft.flush();
  const pending = await draft.prepareSend(draft.read().draft, () => true);
  for (let i = 35; i < 67; i++) f.send(`Message ${i}`, scope.conversationId);
  f.call('apps.discussion.remove', { appId: f.app.id, conversationId: scope.conversationId, messageId: sent[0].receipt.id }, reader);
  await feed.poll({ api: f.api, isCurrent: () => true });
  const removed = feed.read().messages.find(value => value.id === sent[0].receipt.id);
  assert.equal(removed.body, null); assert.notEqual(removed.removedAt, null);
  assert.equal(feed.read().messages.length, 67); assert.deepEqual(draft.read().pending, pending);
  f.reopen(); assert.equal(await feed.poll({ api: f.api, isCurrent: () => true }), 'reset');
  assert.deepEqual(feed.read().messages, []); assert.equal(feed.read().resetRequired, true);
  assert.equal(draft.read().draft.text, pending.args.body); assert.deepEqual(draft.read().pending, pending);
  await feed.load({ api: f.api, isCurrent: () => true }); assert.equal(feed.read().messages.length, 30);
  assert.equal(feed.read().resetRequired, false); assert.deepEqual(draft.read().pending, pending);
});

test('D3 an old conversation response arriving after selecting current cannot replace its empty audience', async t => {
  const f = await model(t), local = localAdapters(t), oldScope = f.scope(); f.send('PRIVATE OLD TEXT', oldScope.conversationId);
  const feed = local.track(createAppDiscussionFeed({ accountId: reader.accountId, appId: f.app.id, entry: f.selected }));
  const arrived = deferred(), answer = deferred();
  const oldLoad = feed.load({ conversationId: oldScope.conversationId, isCurrent: () => true,
    api: { async request(op, args) { const value = await f.api.request(op, args); arrived.resolve(); await answer.promise; return value; } } });
  await arrived.promise; f.rotate(); const next = f.scope(); assert.notEqual(next.conversationId, oldScope.conversationId);
  await feed.load({ api: f.api, isCurrent: () => true }); answer.resolve(); assert.equal(await oldLoad, 'stale');
  assert.equal(feed.read().context.conversationId, next.conversationId); assert.deepEqual(feed.read().messages, []);
  await feed.load({ api: f.api, isCurrent: () => true, conversationId: oldScope.conversationId });
  assert.equal(feed.read().context.mode, 'archive'); assert.equal(feed.read().context.canPost, false);
  assert.deepEqual(feed.read().messages.map(value => value.body), ['PRIVATE OLD TEXT']);
});

test('D3 archive access denial clears loaded text and a late older page cannot restore it across request categories', async t => {
  const f = await model(t), local = localAdapters(t), scope = f.scope();
  for (let i = 0; i < 35; i++) f.send(`Private ${i}`, scope.conversationId);
  const feed = local.track(createAppDiscussionFeed({ accountId: reader.accountId, appId: f.app.id, entry: f.selected }));
  await feed.load({ api: f.api, isCurrent: () => true }); assert.equal(feed.read().messages.length, 30);
  const arrived = deferred(), answer = deferred();
  const older = feed.older({ isCurrent: () => true, api: { async request(op, args) {
    const value = await f.api.request(op, args); arrived.resolve(); await answer.promise; return value;
  } } });
  await arrived.promise; f.revoke();
  await assert.rejects(feed.archives({ api: f.api, isCurrent: () => true }), code('app_unavailable'));
  assert.equal(feed.read().context, null); assert.deepEqual(feed.read().messages, []); assert.deepEqual(feed.read().archives, []);
  answer.resolve(); assert.equal(await older, 'stale'); assert.deepEqual(feed.read().messages, []);
});

test('D3 a send ACK does not invent ordering ahead of an earlier unseen message; changes establish the visible order', async t => {
  const f = await model(t), local = localAdapters(t), scope = f.scope();
  const feed = local.track(createAppDiscussionFeed({ accountId: reader.accountId, appId: f.app.id, entry: f.selected }));
  await feed.load({ api: f.api, isCurrent: () => true });
  f.send('Earlier from another window', scope.conversationId);
  const draft = local.draft(scope); draft.edit({ text: 'My later message', replyTo: null }); await draft.flush();
  const accepted = await dispatchAppDiscussionIntent({ state: draft, expectedDraft: draft.read().draft, beforeCreate: () => true, api: f.api, isCurrent: () => true });
  feed.acceptSend(accepted.pending, accepted.response);
  assert.deepEqual(feed.read().messages, [], 'a receipt is not a full chronological feed snapshot');
  await feed.poll({ api: f.api, isCurrent: () => true });
  assert.deepEqual(feed.read().messages.map(value => value.body), ['Earlier from another window', 'My later message']);
  const ownId = accepted.response.receipt.id;
  const removed = f.call('apps.discussion.remove', { appId: f.app.id, conversationId: scope.conversationId, messageId: ownId }, reader);
  feed.acceptRemoval(removed); feed.acceptSend(accepted.pending, accepted.response);
  assert.equal(feed.read().messages.find(value => value.id === ownId).body, null, 'a late create ACK cannot resurrect a removed message');
});

test('D3 native account mismatch clears discussion context, text and archive metadata while preserving a separately owned draft', async t => {
  const f = await model(t), local = localAdapters(t), first = f.scope(); f.send('Earlier private archive', first.conversationId); f.rotate();
  const current = f.scope(); f.send('Current private body', current.conversationId);
  const draft = local.draft(current); draft.edit({ text: 'My own unsent text', replyTo: null }); await draft.flush();
  const feed = local.track(createAppDiscussionFeed({ accountId: reader.accountId, appId: f.app.id, entry: f.selected }));
  await feed.load({ api: f.api, isCurrent: () => true }); await feed.archives({ api: f.api, isCurrent: () => true });
  assert.equal(feed.read().messages.length, 1); assert.equal(feed.read().archives.length, 1);
  const arrived = deferred(), answer = deferred();
  const archives = feed.archives({ isCurrent: () => true, api: { async request(op, args) {
    const value = await f.api.request(op, args); arrived.resolve(); await answer.promise; return value;
  } } });
  await arrived.promise;
  await assert.rejects(feed.poll({ isCurrent: () => true, api: { async request(op, args) { return f.call(op, args, owner); } } }), code('authentication_required'));
  assert.equal(feed.read().context, null); assert.deepEqual(feed.read().messages, []); assert.deepEqual(feed.read().archives, []);
  answer.resolve(); assert.equal(await archives, 'stale'); assert.deepEqual(feed.read().archives, []);
  assert.equal(draft.read().draft.text, 'My own unsent text');
});

test('D3 replacing a discussion pending slot in another window cannot redirect an old retry or be cleared by its late accepted ACK', async t => {
  const f = await model(t), local = localAdapters(t), scope = f.scope(), a = local.draft(scope);
  a.edit({ text: 'Clicked message A', replyTo: null }); await a.flush();
  const pendingA = await a.prepareSend(a.read().draft, () => true), arrived = deferred(), answer = deferred();
  const sending = dispatchAppDiscussionIntent({ state: a, expectedPending: pendingA, isCurrent: () => true,
    api: { async request(op, args) { const response = await f.api.request(op, args); arrived.resolve(); await answer.promise; return response; } } });
  await arrived.promise;
  const b = local.draft(scope); assert.equal(await b.abandon(pendingA), true);
  b.edit({ text: 'Different message B', replyTo: null }); await b.flush();
  const pendingB = await b.prepareSend(b.read().draft, () => true);
  answer.resolve(); assert.equal((await sending).status, 'superseded');
  assert.deepEqual(b.read().pending, pendingB); assert.equal(b.read().draft.text, 'Different message B');
  let attempts = 0;
  await assert.rejects(dispatchAppDiscussionIntent({ state: b, expectedPending: pendingA, isCurrent: () => true,
    api: { async request(op, args) { attempts++; return f.api.request(op, args); } } }), code('app_discussion_pending_changed'));
  assert.equal(attempts, 0); assert.deepEqual(b.read().pending, pendingB);
  assert.deepEqual(f.context().messages.map(message => message.body), ['Clicked message A']);
});
