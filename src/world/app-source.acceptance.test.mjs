import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { createAppsService } from '../../modules/apps/server/index.mjs';
import { createLocalAppsRuntime } from '../../scripts/agent-modules/local-apps.mjs';
import { createAppSourceState } from './app-source-state.mjs';
import { createAppSettingsState, createAppSettingsDraftState, dispatchAppSettingsIntent } from './app-settings-state.mjs';

// Independent composed contract, with real SQLite receipts and a real connector
// HEAD. Only local storage/locks and response delivery are controllable adapters.
// This is not browser Web Locks, visibility events, DOM or physical mobile QA.
const owner = { accountId: 'source-ui-independent-owner', deviceId: 'source-ui-independent-browser' };
const host = { linkId: 'source-ui-link', hostDeviceId: 'source-ui-host', connectorId: 'source-ui-connector' };
const clone = value => structuredClone(value);
const secret = () => randomBytes(32).toString('base64url');
const digest = value => createHash('sha256').update(value).digest('hex');
const delay = ms => new Promise(done => setTimeout(done, ms));
function deferred() { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return { promise, resolve, reject }; }
async function until(check, label) { const end = Date.now() + 4000; while (!check()) { if (Date.now() > end) throw new Error(`source_ui_timeout:${label}`); await delay(5); } }

function localAdapters() {
  const entries = new Map(), queues = new Map(); let nextGate = null, writesFail = false, serial = 0;
  const storage = { getItem: key => entries.get(key) ?? null,
    setItem(key, value) { if (writesFail) throw new Error('synthetic quota failure'); entries.set(key, value); } };
  const locks = { request(name, callback) {
    const prior = queues.get(name) ?? Promise.resolve(), gate = nextGate; nextGate = null;
    const task = prior.catch(() => {}).then(async () => { if (gate) { gate.entered.resolve(); await gate.release.promise; } return callback(); });
    queues.set(name, task.catch(() => {})); return task;
  } };
  return { state: appId => createAppSettingsState({ accountId: owner.accountId, appId, storage, locks, randomId: () => `source-ui-request-${++serial}` }),
    hold() { nextGate = { entered: deferred(), release: deferred() }; return nextGate; },
    failWrites: value => { writesFail = value; }, generated: () => serial };
}

async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-source-ui-independent-')), sockets = new Set(), upstreamSockets = new Set(), calls = [];
  let service, runtime, clock = Date.now(), monotonic = 100, serial = 0;
  const server = http.createServer((req, res) => { if (!service?.handleRequest(req, res)) { res.writeHead(404); res.end(); } });
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  server.on('upgrade', (req, socket, head) => { if (!service?.handleUpgrade(req, socket, head)) socket.destroy(); });
  const upstream = http.createServer((req, res) => { res.writeHead(200, { 'content-type': 'text/plain' }); res.end(req.url); });
  upstream.on('connection', socket => { upstreamSockets.add(socket); socket.once('close', () => upstreamSockets.delete(socket)); });
  t.after(async () => {
    runtime?.stop(); service?.close(); for (const socket of [...sockets, ...upstreamSockets]) socket.destroy();
    await Promise.all([new Promise(done => server.close(done)), new Promise(done => upstream.close(done))]);
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(directory.split(/[\\/]/u).at(-1), /^soty-source-ui-independent-/u);
    await rm(directory, { recursive: true, force: true });
  });
  await Promise.all([new Promise(done => server.listen(0, '127.0.0.1', done)), new Promise(done => upstream.listen(0, '127.0.0.1', done))]);
  const port = server.address().port, token = secret(), applicationPort = upstream.address().port;
  service = createAppsService({ databasePath: join(directory, 'registry.sqlite'), now: () => clock,
    shellOrigins: [`http://localhost:${port}`], appOriginTemplate: `http://{appId}.legacy.localhost:${port}`, namedAppZone: `http://named.localhost:${port}`,
    actorActive: actor => actor?.accountId === owner.accountId && actor?.deviceId === owner.deviceId,
    isGroupAdmin: (accountId, group) => accountId === owner.accountId && ['original-group', 'new-group'].includes(group),
    canAccessCommunity: () => false,
    authenticateConnector: async value => value.token === token && value.linkId === host.linkId && value.deviceId === host.hostDeviceId && value.connectorId === host.connectorId });
  runtime = createLocalAppsRuntime({ createWebSocket: url => new globalThis.WebSocket(url), httpRequest: http.request, randomSecret: secret, digest,
    encodeBase64: value => Buffer.from(value).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64'), now: () => clock },
  { serverUrl: `http://127.0.0.1:${port}`, identity: host, token, blockedPorts: [] });
  runtime.start(); await until(() => runtime.status().connected, 'connector ready');
  const call = (op, args = {}) => service.execute({ op, args: { ...args, expectedAccountId: owner.accountId }, actor: owner });
  const code = await runtime.claim(); call('apps.claim', { hostDeviceId: host.hostDeviceId, connectorId: host.connectorId, claimCode: code.claimCode });
  const app = call('apps.register', { hostDeviceId: host.hostDeviceId, connectorId: host.connectorId, name: 'Independent source editor',
    port: applicationPort, entryPath: '/original', grants: { accountIds: ['shared-person'], communityIds: ['original-group'] } }).app;
  const inspect = () => call('apps.inspect', { appId: app.id });
  const alias = call('apps.domains.claim', { appId: app.id, slug: 'source-ui-independent', expectedDomainsRevision: 0, requestId: 'source-ui-alias' }).receipt;
  const initial = inspect();
  call('apps.publication.update', { appId: app.id, requestId: 'source-ui-publish', expectedPolicyEpoch: initial.publication.policyEpoch,
    expectedTargetRevision: initial.source.revision, launchPolicy: 'anyone', listed: false, activeDomainIds: [alias.domainId],
    exposureAck: { scope: 'whole-port', targetRevision: initial.source.revision, targetDigest: initial.source.digest, profile: initial.source.profile } });
  const api = { async request(op, args) {
    calls.push({ op, args: clone(args) });
    return op === 'apps.source.prepare' ? service.sourcePreparationExtension.executeAsync({ op, args, actor: owner }) : call(op, args);
  } };
  const model = () => createAppSourceState({ accountId: owner.accountId, appId: app.id, snapshot: inspect(), now: () => monotonic });
  async function checked(path = '/candidate', options = {}) {
    const value = model(); value.patch({ entryPath: path, ...options });
    assert.equal((await value.prepare({ api, isCurrent: () => true })).status, 'ready');
    if (value.read().draft.launchPolicy === 'anyone') value.patch({ exposureConfirmed: true });
    return value;
  }
  async function commit(path) {
    const value = await checked(path), intent = value.promoteIntent();
    return call(intent.op, { ...intent.args, requestId: `source-ui-direct-${++serial}` });
  }
  return { app, inspect, call, api, calls, model, checked, commit, alias, runtime,
    advance(ms) { clock += ms; monotonic += ms; }, advanceLocal(ms) { monotonic += ms; } };
}

test('C2-C source lost ACK survives reload/expired proof and reconciles its receipt separately from a later current source', { timeout: 15_000 }, async t => {
  const f = await fixture(t), local = localAdapters(), durable = local.state(f.app.id), source = await f.checked('/chosen-B#view');
  const sent = [], unreliable = { async request(op, args) { sent.push(clone(args)); await f.api.request(op, args); throw new Error('response lost after commit'); } };
  await assert.rejects(dispatchAppSettingsIntent({ state: durable, ...source.promoteIntent(), api: unreliable, isCurrent: () => true }));
  const pending = durable.read().pending; assert.equal(pending.expectedSource.entryPath, '/chosen-B#view');
  assert.equal(f.inspect().source.entryPath, '/chosen-B#view');
  await f.commit('/another-window-C'); f.advance(31_000); source.dispose();
  const remounted = local.state(f.app.id);
  const result = await dispatchAppSettingsIntent({ state: remounted, expectedPending: pending, api: { async request(op, args) { sent.push(clone(args)); return f.api.request(op, args); } }, isCurrent: () => true });
  assert.equal(result.status, 'accepted'); assert.equal(result.response.replayed, true);
  assert.equal(result.response.receipt.targetRevision, pending.expectedSource.revision);
  assert.ok(result.response.current.activeTargetRevision > result.response.receipt.targetRevision);
  assert.deepEqual(sent[0], sent[1]); assert.equal(Object.hasOwn(sent[1], 'expectedSource'), false);
  assert.equal(local.generated(), 1); assert.equal(remounted.read().pending, null); assert.equal(f.inspect().source.entryPath, '/another-window-C');
});

test('C2-C incompatible tabs cannot replace the shared pending source or silently rebase their edited target', { timeout: 15_000 }, async t => {
  const f = await fixture(t), local = localAdapters(), a = await f.checked('/tab-A'), b = await f.checked('/tab-B');
  const first = local.state(f.app.id), second = local.state(f.app.id), gate = local.hold();
  const pendingA = first.prepare(a.promoteIntent()); await gate.entered.promise;
  const pendingB = second.prepare(b.promoteIntent()); pendingB.catch(() => {}); gate.release.resolve();
  const accepted = await pendingA;
  await assert.rejects(pendingB, { code: 'app_settings_pending_unconfirmed' }); assert.deepEqual(second.read().pending, accepted);
  const committed = await dispatchAppSettingsIntent({ state: first, expectedPending: accepted, api: f.api, isCurrent: () => true }); assert.equal(committed.status, 'accepted');
  b.observe(f.inspect()); assert.equal(b.read().draft.entryPath, '/tab-B'); assert.equal(b.read().conflict, true);
  assert.equal(b.read().canPromote, false); assert.equal(b.read().base.source.revision, 1); assert.equal(local.generated(), 1);
});

test('C2-C a late accepted reply settles only its exact account/app intent and cannot clear a newer source pending', { timeout: 15_000 }, async t => {
  const f = await fixture(t), local = localAdapters(), store = local.state(f.app.id), first = await f.checked('/late-B'), ack = deferred(), admitted = deferred();
  let current = true;
  const flight = dispatchAppSettingsIntent({ state: store, ...first.promoteIntent(), api: { async request(op, args) { const value = await f.api.request(op, args); admitted.resolve(value); return ack.promise; } }, isCurrent: () => current });
  const reply = await admitted.promise, old = store.read().pending; assert.equal(await store.abandon(old), true);
  const next = await f.checked('/newer-C'), newer = await store.prepare(next.promoteIntent()); current = false; ack.resolve(reply);
  assert.equal((await flight).status, 'stale'); assert.deepEqual(store.read().pending, newer);
  assert.equal(store.read().pending.args.expectedAccountId, owner.accountId);
  assert.equal(f.inspect().source.entryPath, '/late-B');
});

test('C2-C exact source receipt mismatches fail closed while an actual matching receipt can settle the same pending', { timeout: 15_000 }, async t => {
  const f = await fixture(t), local = localAdapters(), store = local.state(f.app.id), source = await f.checked('/receipt-B');
  const pending = await store.prepare(source.promoteIntent()), actual = await f.api.request(pending.op, pending.args);
  const corruptions = [value => { value.receipt.preparationKeyHash = '0'.repeat(64); }, value => { value.receipt.targetDigest = '0'.repeat(64); },
    value => { value.receipt.requiredBindingVersion = 1; }, value => { value.receipt.previousTargetRevision++; },
    value => { value.receipt.launchPolicy = 'restricted'; }, value => { value.requestId = 'different-request'; }];
  for (const corrupt of corruptions) {
    const wrong = clone(actual); corrupt(wrong);
    await assert.rejects(store.acknowledge(pending, wrong), { code: 'app_settings_invalid_receipt' }); assert.deepEqual(store.read().pending, pending);
  }
  assert.equal(await store.acknowledge(pending, actual), true); assert.equal(store.read().pending, null);
});

test('C2-C source intent cannot dispatch after proof expiry while waiting for a lock or failed durable storage', { timeout: 15_000 }, async t => {
  const f = await fixture(t), local = localAdapters(), source = await f.checked('/lock-B'), store = local.state(f.app.id);
  const gate = local.hold(), before = f.calls.filter(item => item.op === 'apps.source.promote').length;
  const flight = dispatchAppSettingsIntent({ state: store, ...source.promoteIntent(), api: f.api, isCurrent: () => true }); flight.catch(() => {});
  await gate.entered.promise; f.advance(31_000); gate.release.resolve();
  await assert.rejects(flight, { code: 'apps_source_preparation_expired' }); assert.equal(store.read().pending, null);
  const fresh = await f.checked('/quota-C'); local.failWrites(true);
  await assert.rejects(dispatchAppSettingsIntent({ state: store, ...fresh.promoteIntent(), api: f.api, isCurrent: () => true }), { code: 'app_settings_storage_unavailable' });
  assert.equal(store.read().pending, null); assert.equal(f.calls.filter(item => item.op === 'apps.source.promote').length, before);
  assert.equal(f.inspect().source.entryPath, '/original');
});

test('C2-C hidden/edit/account invalidation cannot accept late prepare and expired review is not a live proof', { timeout: 15_000 }, async t => {
  const f = await fixture(t), source = f.model(), reply = deferred(), checked = deferred(); source.patch({ entryPath: '/slow-B' });
  const flight = source.prepare({ api: { async request(op, args) { const value = await f.api.request(op, args); checked.resolve(); return reply.promise.then(() => value); } }, isCurrent: () => true });
  await checked.promise; source.invalidatePreparation(); source.patch({ entryPath: '/edited-C' }); reply.resolve();
  assert.equal((await flight).status, 'stale'); assert.equal(source.read().preparation, null); assert.equal(source.read().draft.entryPath, '/edited-C');
  await source.prepare({ api: f.api, isCurrent: () => true }); source.patch({ exposureConfirmed: true });
  assert.equal(source.read().canPromote, true); f.advance(30_001);
  assert.equal(source.read().canPromote, false); assert.equal(source.read().draft.exposureConfirmed, true, 'untimed review remains, but is not permission to dispatch an expired proof');
  assert.throws(() => source.promoteIntent(), { code: 'apps_source_preparation_expired' });
  const accountReply = deferred(), begun = deferred();
  let current = true;
  const accountFlight = source.prepare({ api: { async request(op, args) { const value = await f.api.request(op, args); begun.resolve(); return accountReply.promise.then(() => value); } }, isCurrent: () => current });
  await begun.promise; current = false; source.dispose(); accountReply.resolve();
  assert.equal((await accountFlight).status, 'stale'); assert.equal(source.read().canPromote, false);
  assert.equal(f.calls.filter(item => item.op === 'apps.source.promote').length, 0);
});

test('C2-C a name-only write preserves a checked source, but access CAS drift blocks it without rewriting the draft', { timeout: 15_000 }, async t => {
  const f = await fixture(t), source = await f.checked('/renamed-B');
  f.call('apps.update', { appId: f.app.id, expectedRevision: f.inspect().app.revision, name: 'New human name' }); source.observe(f.inspect());
  assert.equal(source.read().conflict, false); assert.equal(source.read().canPromote, true);
  const local = localAdapters(), result = await dispatchAppSettingsIntent({ state: local.state(f.app.id), ...source.promoteIntent(), api: f.api, isCurrent: () => true });
  assert.equal(result.status, 'accepted'); assert.equal(f.inspect().app.name, 'New human name');
  const other = await f.checked('/access-C'), originalBase = other.read().base.publication.policyEpoch;
  f.call('apps.update', { appId: f.app.id, expectedRevision: f.inspect().app.revision, grants: { accountIds: ['shared-person'], communityIds: ['new-group'] } });
  other.observe(f.inspect()); assert.equal(other.read().conflict, true); assert.equal(other.read().draft.entryPath, '/access-C');
  assert.equal(other.read().base.publication.policyEpoch, originalBase); assert.equal(other.read().canPromote, false);
});

test('C2-C resetting only publication keeps name/grants/slug drafts and source promotion saves none of them', { timeout: 15_000 }, async t => {
  const f = await fixture(t), snapshot = f.inspect(), drafts = createAppSettingsDraftState(snapshot), source = f.model();
  drafts.patch({ name: 'Unsubmitted name', communityIds: ['new-group'], slug: 'unsubmitted-address', launchPolicy: 'restricted', activeDomainIds: [] });
  source.patch({ entryPath: '/restricted-B', launchPolicy: 'restricted' }); source.observe(snapshot, { publicationDirty: drafts.read().publicationDirty });
  await assert.rejects(source.prepare({ api: f.api, isCurrent: () => true }), { code: 'app_source_publication_dirty' });
  drafts.resetPublication(); source.observe(snapshot, { publicationDirty: drafts.read().publicationDirty });
  await source.prepare({ api: f.api, isCurrent: () => true });
  const local = localAdapters(), result = await dispatchAppSettingsIntent({ state: local.state(f.app.id), ...source.promoteIntent(), api: f.api, isCurrent: () => true });
  assert.equal(result.status, 'accepted'); drafts.observe(f.inspect());
  assert.equal(drafts.read().draft.name, 'Unsubmitted name'); assert.deepEqual(drafts.read().draft.communityIds, ['new-group']);
  assert.equal(drafts.read().draft.slug, 'unsubmitted-address'); assert.equal(drafts.read().nameDirty, true); assert.equal(drafts.read().grantsDirty, true);
  assert.equal(f.inspect().app.name, snapshot.app.name); assert.deepEqual(f.inspect().app.grants, snapshot.app.grants);
  assert.equal(f.inspect().addresses.aliases.length, 1); assert.equal(f.inspect().publication.launchPolicy, 'restricted');
});

test('C2-C history response is not authority to rebase an edited source; rollback requires a new exact proof and consent', { timeout: 20_000 }, async t => {
  const f = await fixture(t); await f.commit('/history-B');
  const source = f.model(); source.patch({ entryPath: '/draft-C' }); const epoch = source.read().base.publication.policyEpoch;
  await f.commit('/other-window-D');
  assert.equal(await source.loadHistory({ api: f.api, isCurrent: () => true }), 'accepted');
  assert.equal(source.read().history.stale, true); assert.equal(source.read().conflict, true);
  assert.equal(source.read().base.publication.policyEpoch, epoch); assert.equal(source.read().draft.entryPath, '/draft-C');
  source.observe(f.inspect()); source.reset();
  const original = source.read().history.targets.find(item => item.entryPath === '/original'); assert.ok(original);
  source.selectHistory(original); assert.equal(source.read().canPromote, false);
  await source.prepare({ api: f.api, isCurrent: () => true }); assert.equal(source.read().canPromote, false);
  source.patch({ exposureConfirmed: true });
  const local = localAdapters(), result = await dispatchAppSettingsIntent({ state: local.state(f.app.id), ...source.promoteIntent(), api: f.api, isCurrent: () => true });
  assert.equal(result.status, 'accepted'); assert.equal(f.inspect().source.revision, original.revision);
  assert.equal(result.response.receipt.exposureAck.targetDigest, original.digest); assert.equal(f.inspect().source.requiredBindingVersion, 2);
});

test('C2-C actual history cursors stay bounded and later pages do not replace a selected immutable source', { timeout: 25_000 }, async t => {
  const f = await fixture(t);
  for (let number = 0; number < 21; number++) await f.commit(`/history-${number}`);
  const source = f.model(); await source.loadHistory({ api: f.api, isCurrent: () => true });
  const first = source.read().history; assert.ok(first.nextCursor); assert.ok(first.targets.length <= 20);
  const selected = first.targets.at(-1); source.selectHistory(selected);
  const selectedBefore = clone(source.read().draft), base = source.read().base.publication.policyEpoch;
  await source.loadHistory({ api: f.api, isCurrent: () => true, older: true });
  assert.deepEqual(source.read().draft, selectedBefore); assert.equal(source.read().base.publication.policyEpoch, base);
  assert.ok(source.read().history.targets.every(item => item.revision < selected.revision));
  assert.equal(source.read().history.nextCursor, null);
  await source.prepare({ api: f.api, isCurrent: () => true });
  assert.equal(source.read().preparation.target.digest, selected.digest);
  assert.equal(source.read().draft.exposureConfirmed, false);
  assert.equal(f.inspect().source.revision, 22, 'browsing and selecting history did not switch the source');
});

test('C2-C an explicit untimed review rechecks the identical source with a fresh proof before durable promotion', { timeout: 15_000 }, async t => {
  const f = await fixture(t), source = await f.checked('/read-at-my-pace'), reviewed = source.read().preparation;
  source.patch({ exposureConfirmed: false }); f.advance(31_000);
  assert.equal(source.read().remainingMs, 0); assert.equal(source.read().preparation.target.entryPath, '/read-at-my-pace');
  assert.equal(source.read().canRecheckPromote, false);
  const before = f.calls.length;
  await assert.rejects(source.recheckPromotion({ api: f.api, isCurrent: () => true }), { code: 'app_source_review_required' });
  assert.equal(f.calls.length, before, 'without consent the alternate gesture does not even start checking');
  source.patch({ exposureConfirmed: true }); assert.equal(source.read().canRecheckPromote, true);
  const refreshed = await source.recheckPromotion({ api: f.api, isCurrent: () => true });
  assert.equal(refreshed.status, 'ready'); assert.notEqual(refreshed.intent.args.preparationId, reviewed.preparationId);
  assert.deepEqual(refreshed.intent.expectedSource, reviewed.target);
  assert.equal(f.calls.filter(item => item.op === 'apps.source.promote').length, 0, 'the recheck helper has no write effect');
  const local = localAdapters(), accepted = await dispatchAppSettingsIntent({ state: local.state(f.app.id), ...refreshed.intent, api: f.api, isCurrent: () => true });
  assert.equal(accepted.status, 'accepted'); assert.equal(f.inspect().source.entryPath, '/read-at-my-pace');
  assert.equal(accepted.response.receipt.exposureAck.targetDigest, reviewed.target.digest);
});

for (const drift of ['consent', 'tuple-ABA', 'policy', 'hidden', 'account', 'epoch']) {
  test(`C2-C deferred recheck cannot issue a promotion intent after ${drift} drift`, { timeout: 15_000 }, async t => {
    const f = await fixture(t), source = await f.checked('/reviewed-B'), released = deferred(), checked = deferred(); f.advance(31_000);
    let current = true;
    const flight = source.recheckPromotion({ api: { async request(op, args) { const response = await f.api.request(op, args); checked.resolve(); await released.promise; return response; } }, isCurrent: () => current });
    await checked.promise;
    if (drift === 'consent') source.patch({ exposureConfirmed: false });
    else if (drift === 'tuple-ABA') { source.patch({ entryPath: '/temporary-C' }); source.patch({ entryPath: '/reviewed-B' }); }
    else if (drift === 'policy') source.patch({ launchPolicy: 'restricted' });
    else if (drift === 'hidden') source.invalidatePreparation();
    else if (drift === 'account') current = false;
    else { f.call('apps.update', { appId: f.app.id, expectedRevision: f.inspect().app.revision,
      grants: { accountIds: ['new-person'], communityIds: ['original-group'] } }); source.observe(f.inspect()); }
    released.resolve(); const result = await flight;
    assert.equal(result.status, 'stale'); assert.equal(result.intent, undefined);
    assert.equal(f.calls.filter(item => item.op === 'apps.source.promote').length, 0); assert.equal(f.inspect().source.entryPath, '/original');
  });
}

test('C2-C a different checked digest or repeated old preparation cannot promote under the old review', { timeout: 15_000 }, async t => {
  const f = await fixture(t), source = await f.checked('/reviewed-B'); f.advance(31_000);
  const changed = await source.recheckPromotion({ api: { async request(op, args) { const value = await f.api.request(op, args); return { ...value, target: { ...value.target, digest: '0'.repeat(64) } }; } }, isCurrent: () => true });
  assert.equal(changed.status, 'review'); assert.equal(changed.intent, undefined); assert.equal(source.read().draft.exposureConfirmed, false);
  source.reset(); source.patch({ entryPath: '/reviewed-C' }); await source.prepare({ api: f.api, isCurrent: () => true }); source.patch({ exposureConfirmed: true });
  const originalId = source.read().preparation.preparationId; f.advance(31_000);
  const repeated = await source.recheckPromotion({ api: { async request(op, args) { return { ...await f.api.request(op, args), preparationId: originalId }; } }, isCurrent: () => true });
  assert.equal(repeated.status, 'review'); assert.equal(repeated.intent, undefined); assert.equal(source.read().preparation, null);
  assert.equal(f.calls.filter(item => item.op === 'apps.source.promote').length, 0);
});

test('C2-C a competing durable intent or expiry inside its lock prevents the reviewed alternate action from writing', { timeout: 15_000 }, async t => {
  const f = await fixture(t), source = await f.checked('/reviewed-B'), local = localAdapters(), store = local.state(f.app.id); f.advance(31_000);
  const refreshed = await source.recheckPromotion({ api: f.api, isCurrent: () => true }); assert.equal(refreshed.status, 'ready');
  const competing = await store.prepare({ op: 'apps.domains.claim', args: { appId: f.app.id, expectedAccountId: owner.accountId,
    expectedDomainsRevision: f.inspect().addresses.revision, slug: 'saved-other-command' } });
  await assert.rejects(dispatchAppSettingsIntent({ state: store, ...refreshed.intent, api: f.api, isCurrent: () => true }), { code: 'app_settings_pending_unconfirmed' });
  assert.deepEqual(store.read().pending, competing); assert.equal(await store.abandon(competing), true);
  const gate = local.hold(), flight = dispatchAppSettingsIntent({ state: store, ...refreshed.intent, api: f.api, isCurrent: () => true }); flight.catch(() => {});
  await gate.entered.promise; f.advance(31_000); gate.release.resolve();
  await assert.rejects(flight, { code: 'apps_source_preparation_expired' }); assert.equal(store.read().pending, null);
  assert.equal(f.calls.filter(item => item.op === 'apps.source.promote').length, 0); assert.equal(f.inspect().source.entryPath, '/original');
});

test('C2-C retry of the displayed source cannot dispatch a different pending substituted while its lock was waiting', { timeout: 15_000 }, async t => {
  const f = await fixture(t), local = localAdapters(), sourceA = await f.checked('/displayed-A'), sourceB = await f.checked('/unseen-B');
  const viewStore = local.state(f.app.id), otherTab = local.state(f.app.id);
  const displayed = await viewStore.prepare(sourceA.promoteIntent());
  const gate = local.hold(), replace = otherTab.abandon(displayed); await gate.entered.promise;
  const replacement = otherTab.prepare(sourceB.promoteIntent());
  const retry = dispatchAppSettingsIntent({ state: viewStore, expectedPending: displayed, api: f.api, isCurrent: () => true }); retry.catch(() => {});
  gate.release.resolve(); assert.equal(await replace, true); const newest = await replacement;
  await assert.rejects(retry, { code: 'app_settings_pending_changed' });
  assert.deepEqual(viewStore.read().pending, newest);
  assert.equal(f.calls.filter(item => item.op === 'apps.source.promote').length, 0);
  assert.equal(f.inspect().source.entryPath, '/original');
});

test('C2-C retry keeps the displayed source identity when another tab has already replaced the local slot', { timeout: 15_000 }, async t => {
  const f = await fixture(t), local = localAdapters(), viewStore = local.state(f.app.id), otherTab = local.state(f.app.id);
  const source = await f.checked('/displayed-source'), displayed = await viewStore.prepare(source.promoteIntent());
  assert.equal(await otherTab.abandon(displayed), true);
  const unseen = await otherTab.prepare({ op: 'apps.domains.claim', args: { appId: f.app.id, expectedAccountId: owner.accountId,
    expectedDomainsRevision: f.inspect().addresses.revision, slug: 'unseen-new-address' } });
  const before = f.calls.length;
  await assert.rejects(dispatchAppSettingsIntent({ state: viewStore, expectedPending: displayed, api: f.api, isCurrent: () => true }), { code: 'app_settings_pending_changed' });
  assert.equal(f.calls.length, before); assert.deepEqual(viewStore.read().pending, unseen);
  assert.equal(f.inspect().addresses.aliases.length, 1, 'a source retry must never claim a different, unseen address');
});

test('C2-C omitted displayed identity or an already-cleared retry slot cannot perform any API action', { timeout: 15_000 }, async t => {
  const f = await fixture(t), local = localAdapters(), store = local.state(f.app.id), source = await f.checked('/displayed-source');
  const displayed = await store.prepare(source.promoteIntent()), before = f.calls.length;
  await assert.rejects(dispatchAppSettingsIntent({ state: store, api: f.api, isCurrent: () => true }), { code: 'app_settings_pending_changed' });
  assert.deepEqual(store.read().pending, displayed); assert.equal(f.calls.length, before);
  assert.equal(await store.abandon(displayed), true);
  await assert.rejects(dispatchAppSettingsIntent({ state: store, expectedPending: displayed, api: f.api, isCurrent: () => true }), { code: 'app_settings_pending_changed' });
  assert.equal(store.read().pending, null); assert.equal(f.calls.length, before); assert.equal(f.inspect().source.entryPath, '/original');
});

test('C2-C unchanged clean inspection during history loading must not discard the actual history response', { timeout: 15_000 }, async t => {
  const f = await fixture(t); await f.commit('/history-B');
  const source = f.model();
  for (const kind of ['initial', 'latest']) {
    const begun = deferred(), response = deferred();
    const loading = source.loadHistory({ api: { async request(op, args) { const value = await f.api.request(op, args); begun.resolve(); await response.promise; return value; } }, isCurrent: () => true });
    await begun.promise;
    // Actual view re-renders when loading starts, and observes the unchanged
    // clean inspection. That observation is neither reset nor cancellation.
    source.observe(f.inspect()); response.resolve();
    assert.equal(await loading, 'accepted', `${kind} history was spuriously cancelled`);
    assert.deepEqual(source.read().history.targets.map(value => value.entryPath), ['/history-B', '/original']);
    assert.equal(source.read().history.loading, false);
  }
  assert.equal(f.inspect().source.entryPath, '/history-B');
});
