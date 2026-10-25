import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { appSourcePreparationRemaining, createAppSourceState, normalizeAppSourceTarget } from './app-source-state.mjs';
import { createAppSettingsState, dispatchAppSettingsIntent } from './app-settings-state.mjs';

const appId = `app-${'1'.repeat(32)}`, domainId = `dom_${'a'.repeat(32)}`, profile = 'soty.relay-restricted.v1';
const sha = value => createHash('sha256').update(value).digest('hex');
const copy = value => structuredClone(value);
function inspection() {
  return { schema: 'soty.app-inspection.v1', checkedAt: 100_000,
    app: { id: appId, name: 'Проект', state: 'enabled', revision: 4, grants: { accountIds: ['owner'], communityIds: ['group'] } },
    publication: { policyEpoch: 8, launchPolicy: 'restricted', listed: false, activeDomainIds: [domainId], activeTargetRevision: 2 },
    addresses: { revision: 1, aliases: [{ id: domainId, state: 'bound', active: true }] },
    source: { revision: 2, digest: 'b'.repeat(64), profile, hostDeviceId: 'host-a', connectorId: 'connector-a', deviceName: 'Ноутбук',
      port: 5000, entryPath: '/#/доска', requiredBindingVersion: 2, binding: { state: 'bound' },
      observation: { state: 'responding', observedAt: 90_000, freshUntil: 135_000, evidence: 'connector-v2-observation' } },
    actions: { canEdit: true, canPublish: true } };
}
function target(overrides = {}) {
  return { revision: 5, digest: 'c'.repeat(64), profile, hostDeviceId: 'host-b', connectorId: 'connector-b', deviceName: 'Рабочий компьютер',
    port: 6000, entryPath: '/board?tag=a%2Bb#item', ...overrides };
}
function prepared(args, value = target(), overrides = {}) {
  return { schema: 'soty.app-source-preparation.v1', appId, preparationId: 'p'.repeat(43), requiredBindingVersion: 2,
    expectedPolicyEpoch: args.expectedPolicyEpoch, expectedTargetRevision: args.expectedTargetRevision, target: value,
    checkedAt: 100_000, expiresAt: 125_000, ...overrides };
}
function fixture(snapshot = inspection()) {
  let time = 100, active = true;
  const source = createAppSourceState({ accountId: 'owner', appId, snapshot, now: () => time });
  const change = () => source.patch({ hostDeviceId: 'host-b', connectorId: 'connector-b', port: '6000', entryPath: '/board?tag=a%2Bb#item' });
  return { source, change, setTime(value) { time = value; }, setActive(value) { active = value; },
    isCurrent: () => active, async prepare(value = target(), responseOverride = {}) {
      return source.prepare({ isCurrent: () => active, api: { request: async (op, args) => {
        assert.equal(op, 'apps.source.prepare'); return prepared(args, value, responseOverride);
      } } });
    } };
}
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };
function storeFixture(locks = { request: async (_key, callback) => callback() }) {
  const values = new Map(); let seq = 0;
  const options = { accountId: 'owner', appId, storage: { getItem: key => values.get(key) ?? null, setItem: (key, value) => values.set(key, value) }, locks,
    randomId: () => `request-${++seq}` };
  return { options, values, state: createAppSettingsState(options) };
}
function receipt(pending) {
  const a = pending.args, t = pending.expectedSource;
  return { requestId: a.requestId, replayed: false, receipt: { schema: `soty.app-source-receipt.v${a.nativeExposureAck || a.nativeEntryAction ? 2 : 1}`, namespace: `apps.source.promote.v${a.nativeExposureAck || a.nativeEntryAction ? 2 : 1}`,
    appId, requestKeyHash: sha(a.requestId), preparationKeyHash: sha(a.preparationId), previousTargetRevision: a.expectedTargetRevision,
    targetRevision: t.revision, targetDigest: t.digest, profile: t.profile, requiredBindingVersion: 2, policyEpoch: a.expectedPolicyEpoch + 1,
    launchPolicy: a.launchPolicy, listed: a.listed, exposureAck: a.exposureAck ?? null, committedAt: 101_000,
    ...(a.nativeEntryAction ? { nativeEntryAction: a.nativeEntryAction } : {}),
    ...(a.nativeExposureAck ? { nativeEntry: { ...pending.expectedNativeEntry, target: { revision: a.nativeExposureAck.targetRevision,
      digest: a.nativeExposureAck.targetDigest, profile: a.nativeExposureAck.profile }, exposureAck: a.nativeExposureAck } } : {}) },
    current: { appId, policyEpoch: a.expectedPolicyEpoch + 1, requiredBindingVersion: 2, activeTargetRevision: t.revision } };
}

function pairedFixture() {
  const snapshot = inspection(), nativeTarget = target({ revision: 1, digest: 'a'.repeat(64), profile,
    hostDeviceId: 'host-a', connectorId: 'connector-a', port: 5001, entryPath: '/' });
  Object.assign(snapshot.source, { profile: 'soty.selected-human-embed.v2', entryPath: '/embed' });
  Object.assign(snapshot.publication, { launchPolicy: 'anyone', listed: true, nativeEntry: {
    schema: 'soty.native-entry.v1', origin: 'https://native.apps.test', domainId, target: {
      revision: 1, digest: nativeTarget.digest, profile, port: 5001, entryPath: '/' }, runtimeReady: false } });
  const f = fixture(snapshot), next = target({ profile: snapshot.source.profile, hostDeviceId: 'host-a', connectorId: 'connector-a', entryPath: '/embed' });
  const pair = { origin: snapshot.publication.nativeEntry.origin, domainId, target: nativeTarget };
  return { ...f, snapshot, next, pair };
}

test('paired replacement defaults to keep, independently reviews both entries, then recovers its exact durable v2 receipt', async () => {
  const f = pairedFixture(), store = storeFixture(); f.source.patch({ port: '6000' });
  assert.equal(f.source.read().draft.nativeEntryMode, 'keep');
  await f.source.prepare({ isCurrent: () => true, api: { request: async (_op, args) => {
    assert.deepEqual(args.nativeEntry, { origin: f.pair.origin, targetRevision: 1 });
    return prepared(args, f.next, { nativeEntry: f.pair });
  } } });
  f.source.patch({ exposureConfirmed: true }); assert.equal(f.source.read().canPromote, false);
  assert.throws(() => f.source.promoteIntent(), { code: 'app_native_exposure_ack_required' });
  f.source.patch({ nativeConfirmed: true }); const intent = f.source.promoteIntent();
  assert.equal(intent.args.exposureAck.scope, 'authenticated-workspace'); assert.equal(intent.args.nativeExposureAck.scope, 'whole-port');
  for (const patch of [
    { ...intent, args: { ...intent.args, nativeExposureAck: { ...intent.args.nativeExposureAck, targetDigest: [intent.args.nativeExposureAck.targetDigest] } } },
    { ...intent, expectedNativeEntry: { ...intent.expectedNativeEntry, domainId: [domainId] } },
  ]) { await assert.rejects(store.state.prepare(patch)); assert.equal(store.state.read().pending, null); }
  await assert.rejects(dispatchAppSettingsIntent({ ...intent, state: store.state, isCurrent: () => true,
    api: { request: async () => { throw Error('lost ACK'); } } }));
  const reopened = createAppSettingsState(store.options), pending = reopened.read().pending;
  assert.deepEqual(pending.expectedNativeEntry, { origin: f.pair.origin, domainId });
  const valid = receipt(pending);
  for (const mutate of [r => { r.receipt.nativeEntry.origin = 'https://other.apps.test'; },
    r => { r.receipt.nativeEntry.target.digest = 'f'.repeat(64); }, r => { delete r.receipt.nativeEntry; },
    r => { r.receipt.schema = 'soty.app-source-receipt.v1'; }]) {
    const bad = copy(valid); mutate(bad);
    await assert.rejects(reopened.acknowledge(pending, bad), { code: 'app_settings_invalid_receipt' });
    assert.deepEqual(reopened.read().pending, pending);
  }
  assert.equal(await reopened.acknowledge(pending, valid), true); assert.equal(reopened.read().pending, null);
});

test('explicit removal binds exact current Native URL/pins, needs its own confirmation and never inherits the old ACK', async () => {
  const f = pairedFixture(), store = storeFixture(); f.source.patch({ nativeEntryMode: 'remove' });
  const removal = { action: 'remove', origin: f.pair.origin, targetRevision: 1, targetDigest: f.pair.target.digest };
  await f.source.prepare({ isCurrent: () => true, api: { request: async (_op, args) => {
    assert.equal(args.nativeEntry, undefined); assert.equal(args.targetRevision, f.snapshot.source.revision);
    assert.deepEqual(args.nativeEntryAction, removal);
    return prepared(args, normalizeAppSourceTarget(Object.fromEntries(Object.keys(target()).map(k => [k, f.snapshot.source[k]]))), { nativeEntryAction: removal });
  } } });
  f.source.patch({ exposureConfirmed: true }); assert.equal(f.source.read().canPromote, false);
  f.source.patch({ nativeConfirmed: true }); const intent = f.source.promoteIntent();
  assert.deepEqual(intent.args.nativeEntryAction, removal); assert.equal(intent.args.nativeExposureAck, undefined);
  await assert.rejects(store.state.prepare({ ...intent, args: { ...intent.args, nativeEntryAction: { ...removal, targetDigest: [removal.targetDigest] } } }));
  assert.equal(store.state.read().pending, null);
  const pending = await store.state.prepare(intent), valid = receipt(pending), bad = copy(valid);
  bad.receipt.nativeEntryAction.targetDigest = 'f'.repeat(64);
  await assert.rejects(store.state.acknowledge(pending, bad), { code: 'app_settings_invalid_receipt' });
  assert.equal(await store.state.acknowledge(pending, valid), true);
});

test('a changed Native entry invalidates an in-flight pair review and cannot inherit earlier consent', async () => {
  const f = pairedFixture(), gate = deferred(); f.source.patch({ port: '6000' });
  const wait = f.source.prepare({ isCurrent: () => true, api: { request: () => gate.promise } });
  const changed = copy(f.snapshot); changed.publication.nativeEntry.target.digest = 'e'.repeat(64); f.source.observe(changed);
  gate.resolve({}); assert.deepEqual(await wait, { status: 'stale' });
  assert.equal(f.source.read().draft.nativeConfirmed, false); assert.equal(f.source.read().canPromote, false);
});

test('installed selected option changes only profile/path and retains its exact profile through durable retry and readback', async () => {
  for (const selectedProfile of ['soty.selected-human-embed.v1', 'soty.selected-human-embed.v2']) {
    const snapshot = inspection(); snapshot.source.entryPath = '/embed';
    snapshot.source.integrations = [{ profile: selectedProfile, entryPath: '/embed' }];
    const f = fixture(snapshot), selected = target({ ...snapshot.source, revision: 5, digest: 'c'.repeat(64), profile: selectedProfile });
    const expected = normalizeAppSourceTarget(Object.fromEntries(Object.keys(target()).map(key => [key, selected[key]])));
    f.source.patch({ profile: selectedProfile }); assert.equal(f.source.read().dirty, true); assert.equal(f.source.read().canPrepare, true);
    await f.source.prepare({ isCurrent: () => true, api: { request: async (_op, args) => {
      assert.equal(args.source.profile, selectedProfile); assert.equal(args.source.port, snapshot.source.port);
      return prepared(args, expected);
    } } });
    const store = storeFixture(), intent = f.source.promoteIntent();
    await assert.rejects(dispatchAppSettingsIntent({ ...intent, state: store.state, isCurrent: () => true,
      api: { request: async () => { throw Error('synthetic lost reply'); } } }));
    const pending = store.state.read().pending; assert.equal(pending.expectedSource.profile, selectedProfile);
    f.source.dispose(); const resumed = createAppSettingsState(store.options);
    const accepted = await dispatchAppSettingsIntent({ state: resumed, expectedPending: resumed.read().pending, isCurrent: () => true,
      api: { request: async (op, args) => { assert.equal(op, pending.op); assert.deepEqual(args, pending.args); return receipt(pending); } } });
    assert.equal(accepted.status, 'accepted'); assert.equal(resumed.read().pending, null);
    Object.assign(snapshot.source, expected); delete snapshot.source.integrations; snapshot.publication.activeTargetRevision = expected.revision;
    const readback = fixture(snapshot).source.read(); assert.equal(readback.draft.profile, selectedProfile); assert.equal(readback.dirty, false);
    assert.equal(readback.canPrepare, false); // Never silently offer a relay downgrade.
  }
});

test('installed hints keep the declared device/path and cannot survive withdrawal during an in-flight check', async () => {
  const snapshot = inspection(), selectedProfile = 'soty.selected-human-embed.v2';
  snapshot.source.entryPath = '/embed'; snapshot.source.integrations = [{ profile: selectedProfile, entryPath: '/embed' }];
  const f = fixture(snapshot); f.source.patch({ profile: selectedProfile });
  f.source.patch({ port: '6000' }); assert.equal(f.source.read().canPrepare, true);
  f.source.reset(); f.source.patch({ profile: selectedProfile });
  for (const patch of [{ hostDeviceId: 'another' }, { connectorId: 'another' }, { entryPath: '/' }]) {
    f.source.patch(patch); assert.equal(f.source.read().canPrepare, false); f.source.reset(); f.source.patch({ profile: selectedProfile });
  }
  const wait = deferred(), flight = f.source.prepare({ isCurrent: () => true, api: { request: () => wait.promise } });
  const withdrawn = copy(snapshot); delete withdrawn.source.integrations; f.source.observe(withdrawn);
  wait.resolve({}); assert.deepEqual(await flight, { status: 'stale' }); assert.equal(f.source.read().canPrepare, false);
  for (const integrations of [[null], [{ profile: selectedProfile, entryPath: '/embed', tenantId: 'private' }],
    [{ profile: 'future', entryPath: '/embed' }], Array(2).fill(snapshot.source.integrations[0])]) {
    assert.throws(() => fixture({ ...snapshot, source: { ...snapshot.source, integrations } }), { code: 'app_source_invalid_scope' });
  }
});

test('bounded target projection preserves raw Unicode/hash paths but rejects readiness, proofs and unknown profiles', () => {
  assert.deepEqual(normalizeAppSourceTarget(target()), target());
  for (const bad of [{ ...target(), expiresAt: 123 }, { ...target(), proof: {} }, { ...target(), entryPath: 'x'.repeat(8193) },
    { ...target(), profile: 'future' }, { ...target(), deviceName: 'x'.repeat(181) }, { ...target(), port: 65536 }, { ...target(), digest: ['c'.repeat(64)] }]) {
    assert.throws(() => normalizeAppSourceTarget(bad), { code: 'app_source_invalid_target' });
  }
});

test('current tuple and current history target are no-ops; malformed raw inputs stay editable until validation', async () => {
  const f = fixture(); assert.equal(f.source.read().canPrepare, false);
  const current = normalizeAppSourceTarget(Object.fromEntries(Object.keys(target()).map(key => [key, inspection().source[key]])));
  f.source.selectHistory(current); assert.equal(f.source.read().canPrepare, false);
  await assert.rejects(f.prepare(current), { code: 'app_source_unchanged' });
  f.change(); f.source.patch({ port: '' }); assert.equal(f.source.read().draft.port, '');
  await assert.rejects(f.prepare(), { code: 'invalid_app_port' });
  f.source.patch({ port: '49424' }); await assert.rejects(f.prepare(), { code: 'invalid_app_port' });
  f.source.patch({ port: '6000', entryPath: '/a/../_soty/session' }); await assert.rejects(f.prepare(), { code: 'invalid_app_path' });
});

test('prepare captures exact identity and source, accepts non-consecutive immutable revision and shortens TTL by RTT', async () => {
  const f = fixture(); f.change(); let sent;
  assert.deepEqual(await f.source.prepare({ isCurrent: f.isCurrent, api: { async request(op, args) {
    sent = copy(args); f.setTime(1100); return prepared(args, target());
  } } }), { status: 'ready' });
  assert.deepEqual(sent, { appId, expectedAccountId: 'owner', expectedPolicyEpoch: 8, expectedTargetRevision: 2,
    source: { hostDeviceId: 'host-b', connectorId: 'connector-b', port: 6000, entryPath: '/board?tag=a%2Bb#item' } });
  assert.equal(f.source.read().remainingMs, 24_000);
  const view = f.source.read(); view.preparation.target.port = 9000; view.draft.port = '9000';
  assert.equal(f.source.promoteIntent().expectedSource.port, 6000);
  f.setTime(25_100); assert.equal(f.source.read().canPromote, false);
  assert.equal(f.source.read().preparation.target.port, 6000); // Expired review remains readable.
  f.setTime(1100); assert.equal(f.source.read().canPromote, false); // A broken clock never resurrects proof.
});

test('preparation TTL is server relative, rejects invalid elapsed values and never exceeds thirty seconds', () => {
  const value = { checkedAt: 2_000_000_000, expiresAt: 2_000_000_400 };
  assert.equal(appSourcePreparationRemaining(value, 150), 250);
  assert.equal(appSourcePreparationRemaining(value, 500), 0);
  assert.equal(appSourcePreparationRemaining({ checkedAt: 1, expiresAt: 90_000 }, 100), 29_900);
  for (const elapsed of [-1, NaN, Infinity]) assert.equal(appSourcePreparationRemaining(value, elapsed), 0);
  for (const checkedAt of [null, undefined, -1, 1.5]) assert.equal(appSourcePreparationRemaining({ ...value, checkedAt }, 0), 0);
});

test('editing during a real deferred prepare fences its response and retains the outstanding operation slot', async () => {
  const f = fixture(); f.change(); const wait = deferred(); let args;
  const pending = f.source.prepare({ isCurrent: f.isCurrent, api: { request(_op, value) { args = value; return wait.promise; } } });
  f.source.patch({ port: '6001' }); assert.equal(f.source.read().preparing, true);
  await assert.rejects(f.prepare(), { code: 'apps_source_preparation_capacity' });
  wait.resolve(prepared(args)); assert.deepEqual(await pending, { status: 'stale' });
  assert.equal(f.source.read().preparing, false); assert.equal(f.source.read().preparation, null);
  assert.equal(f.source.read().draft.port, '6001');
});

test('hidden, disposed and changed-account prepare responses/errors never become current UI results', async () => {
  for (const reason of ['hidden', 'dispose', 'account']) {
    const f = fixture(); f.change(); const wait = deferred();
    const pending = f.source.prepare({ isCurrent: f.isCurrent, api: { request() { return wait.promise; } } });
    if (reason === 'hidden') f.source.invalidatePreparation('hidden');
    else if (reason === 'dispose') f.source.dispose(); else f.setActive(false);
    wait.reject(new Error('late offline')); assert.deepEqual(await pending, { status: 'stale' });
    assert.equal(f.source.read().canPromote, false);
  }
});

test('malformed or mismatched prepare responses cannot create a promotable target', async () => {
  for (const invalid of [target({ hostDeviceId: 'other' }), target({ port: 6001 }), target({ entryPath: '/elsewhere' }),
    target({ revision: 2 }), target({ profile: 'unknown' })]) {
    const f = fixture(); f.change(); await assert.rejects(f.prepare(invalid)); assert.equal(f.source.read().preparation, null);
  }
  const f = fixture(); f.change(); await assert.rejects(f.prepare(target(), { expectedPolicyEpoch: 9 }), { code: 'app_source_invalid_preparation' });
  await assert.rejects(f.prepare(target(), { preparationId: ['p'.repeat(43)] }), { code: 'app_source_invalid_preparation' });
});

test('name-only refresh preserves proof; source/CAS and dirty publication invalidate it without rebasing source edits', async () => {
  const f = fixture(); f.change(); await f.prepare();
  const named = inspection(); named.app.name = 'Новое имя'; named.app.revision++;
  f.source.observe(named); assert.equal(f.source.read().canPromote, true);
  f.source.observe(named, { publicationDirty: true }); assert.equal(f.source.read().canPrepare, false); assert.equal(f.source.read().preparation, null);
  f.source.observe(named, { publicationDirty: false }); await f.prepare();
  const changed = copy(named); changed.publication.policyEpoch++;
  f.source.observe(changed); assert.equal(f.source.read().conflict, true); assert.equal(f.source.read().draft.port, '6000');
  assert.equal(f.source.read().base.publication.policyEpoch, 8);
  f.source.reset(); assert.equal(f.source.read().conflict, false); assert.equal(f.source.read().base.publication.policyEpoch, 9);
});

test('public consent belongs to the prepared candidate; rollback keeps target identity until a tuple edit', async () => {
  const value = inspection(); value.publication.launchPolicy = 'anyone'; value.publication.listed = true;
  const f = fixture(value), historical = target({ revision: 1, digest: 'd'.repeat(64) });
  f.source.selectHistory(historical); let sent;
  await f.source.prepare({ isCurrent: f.isCurrent, api: { async request(_op, args) { sent = args; return prepared(args, historical); } } });
  assert.equal(sent.targetRevision, 1); assert.equal(sent.expectedTargetRevision, 2); assert.equal(Object.hasOwn(sent, 'source'), false);
  assert.throws(() => f.source.promoteIntent(), { code: 'app_exposure_ack_required' });
  f.source.patch({ exposureConfirmed: true }); const intent = f.source.promoteIntent();
  assert.equal(intent.args.exposureAck.targetRevision, 1); assert.equal(intent.args.exposureAck.targetDigest, historical.digest);
  assert.equal(intent.args.listed, true); assert.equal(intent.beforeCreate(), true);
  f.source.patch({ launchPolicy: 'restricted' }); assert.equal(f.source.read().draft.mode, 'history'); assert.equal(f.source.read().preparation, null);
  await f.prepare(historical); assert.equal(f.source.promoteIntent().args.listed, false);
  f.source.patch({ port: '6001' }); assert.equal(f.source.read().draft.mode, 'new'); assert.equal(f.source.read().draft.targetRevision, null);
});

test('history paging replaces a bounded page, sends opaque cursor unchanged and does not duplicate the current row', async () => {
  const f = fixture(), calls = [];
  const historyApi = { async request(_op, args) {
    calls.push(copy(args)); const older = args.cursor !== undefined;
    return { schema: 'soty.app-source-history.v1', appId, policyEpoch: 8, activeTargetRevision: 2, requiredBindingVersion: 2,
      targets: (older ? [1] : [3, 2]).map(revision => ({ ...target({ revision, digest: String(revision).repeat(64) }), createdAt: revision })),
      nextCursor: older ? null : 'opaque_next_ABC' };
  } };
  await f.source.loadHistory({ api: historyApi, isCurrent: f.isCurrent });
  f.source.selectHistory(f.source.read().history.targets[1]); assert.equal(f.source.read().canPrepare, false);
  await f.source.loadHistory({ api: historyApi, isCurrent: f.isCurrent, older: true });
  assert.deepEqual(f.source.read().history.targets.map(value => value.revision), [1]);
  assert.equal(calls[1].cursor, 'opaque_next_ABC'); assert.equal(calls[0].limit, 20);
  assert.equal(f.source.read().draft.targetRevision, 2); // Paging does not select another configuration.
});

test('history revealing newer authority invalidates proof without silently changing the existing source base', async () => {
  const f = fixture(); f.change(); await f.prepare();
  await f.source.loadHistory({ isCurrent: f.isCurrent, api: { async request() {
    return { schema: 'soty.app-source-history.v1', appId, policyEpoch: 9, activeTargetRevision: 2, requiredBindingVersion: 2,
      targets: [{ ...target(), createdAt: 1 }], nextCursor: null };
  } } });
  assert.equal(f.source.read().history.stale, true); assert.equal(f.source.read().conflict, true);
  assert.equal(f.source.read().preparation, null); assert.equal(f.source.read().base.publication.policyEpoch, 8);
  const next = inspection(); next.publication.policyEpoch = 9; f.source.observe(next);
  assert.equal(f.source.read().conflict, true); assert.equal(f.source.read().draft.port, '6000');
});

test('history response after explicit reset is stale and cannot rewrite its page', async () => {
  const f = fixture(), wait = deferred();
  const reading = f.source.loadHistory({ isCurrent: f.isCurrent, api: { request: () => wait.promise } });
  f.source.reset(); wait.resolve({ malformed: true });
  assert.equal(await reading, 'stale'); assert.equal(f.source.read().history.loading, false); assert.deepEqual(f.source.read().history.targets, []);
});

test('unchanged inspection observations during history loading preserve the independent bounded page request', async () => {
  const f = fixture(), wait = deferred();
  const reading = f.source.loadHistory({ isCurrent: f.isCurrent, api: { request: () => wait.promise } });
  // Actual view renders immediately after starting the request, then again on
  // ordinary refreshes. Every render observes the committed inspection.
  for (let index = 0; index < 3; index++) f.source.observe(inspection(), { publicationDirty: false });
  const targets = [2, 1].map(revision => ({ ...target({ revision, digest: String(revision).repeat(64) }), createdAt: revision }));
  wait.resolve({ schema: 'soty.app-source-history.v1', appId, policyEpoch: 8, activeTargetRevision: 2,
    requiredBindingVersion: 2, targets, nextCursor: null });
  assert.equal(await reading, 'accepted'); assert.deepEqual(f.source.read().history.targets, targets);
  assert.equal(f.source.read().history.loading, false); assert.equal(f.source.read().dirty, false);
});

test('accepted promotion clears only its matching source draft; a newer candidate survives with original CAS conflict', async () => {
  for (const editLater of [false, true]) {
    const f = fixture(); f.change(); await f.prepare(); const intent = f.source.promoteIntent();
    const pending = { op: intent.op, args: { ...intent.args, requestId: 'accepted' }, expectedSource: intent.expectedSource };
    if (editLater) f.source.patch({ port: '7000' });
    const next = inspection(); next.publication.policyEpoch++; next.publication.activeTargetRevision = 5; Object.assign(next.source, target());
    f.source.observe(next, { completed: pending });
    assert.equal(f.source.read().dirty, editLater); assert.equal(f.source.read().draft.port, editLater ? '7000' : '6000');
    assert.equal(f.source.read().base.publication.policyEpoch, editLater ? 8 : 9);
  }
});

test('a preparation expiring while waiting for the local Web Lock produces neither pending storage nor network dispatch', async () => {
  const f = fixture(); f.change(); await f.prepare(); const intent = f.source.promoteIntent();
  const wait = deferred(), storage = storeFixture({ async request(_key, callback) { await wait.promise; return callback(); } });
  let calls = 0;
  const sending = dispatchAppSettingsIntent({ ...intent, state: storage.state, isCurrent: f.isCurrent, api: { request() { calls++; } } });
  f.setTime(30_101); wait.resolve();
  await assert.rejects(sending, { code: 'apps_source_preparation_expired' }); assert.equal(storage.state.read().pending, null); assert.equal(calls, 0);
});

test('source pending persists only bounded expected target metadata and sends only unchanged wire args after reload', async () => {
  const f = fixture(); f.change(); await f.prepare(); const store = storeFixture(), intent = f.source.promoteIntent();
  const pending = await store.state.prepare(intent); f.setTime(50_000); f.source.dispose();
  const resumed = createAppSettingsState(store.options); let sent;
  const result = await dispatchAppSettingsIntent({ state: resumed, expectedPending: resumed.read().pending, isCurrent: () => true, api: { async request(op, args) {
    sent = { op, args }; return receipt(pending);
  } } });
  assert.equal(result.status, 'accepted'); assert.deepEqual(sent, { op: pending.op, args: pending.args });
  assert.equal(Object.hasOwn(sent.args, 'expectedSource'), false); assert.equal(resumed.read().pending, null);
  assert.equal(Object.hasOwn(pending, 'beforeCreate'), false); assert.equal(Object.hasOwn(pending.expectedSource, 'expiresAt'), false);
});

test('wrong source receipt pins, proof identity, old target, policy or binding floor never clear pending', async () => {
  const f = fixture(); f.change(); await f.prepare(); const store = storeFixture(); const pending = await store.state.prepare(f.source.promoteIntent());
  for (const patch of [{ preparationKeyHash: sha('another') }, { previousTargetRevision: 1 }, { targetRevision: 1 },
    { targetDigest: 'd'.repeat(64) }, { requiredBindingVersion: 1 }, { policyEpoch: 10 }, { launchPolicy: 'anyone' }, { listed: true }]) {
    const wrong = receipt(pending); Object.assign(wrong.receipt, patch);
    await assert.rejects(store.state.acknowledge(pending, wrong), { code: 'app_settings_invalid_receipt' });
    assert.equal(store.state.read().pending.args.requestId, pending.args.requestId);
  }
  const historical = receipt(pending); historical.replayed = true; historical.current.policyEpoch += 4; historical.current.activeTargetRevision = 1;
  assert.equal(await store.state.acknowledge(pending, historical), true);
});

test('source metadata is mandatory and an asynchronous beforeCreate cannot authorize a new local command', async () => {
  const f = fixture(); f.change(); await f.prepare(); const intent = f.source.promoteIntent(), store = storeFixture();
  await assert.rejects(store.state.prepare({ op: intent.op, args: intent.args }), { code: 'app_source_invalid_target' });
  await assert.rejects(store.state.prepare({ ...intent, beforeCreate: async () => true }), { code: 'apps_source_preparation_expired' });
  assert.equal(store.state.read().pending, null);
});

test('untimed expired review accepts deliberate consent and explicit fresh recheck returns an intent without dispatching it', async () => {
  const value = inspection(); value.publication.launchPolicy = 'anyone'; value.publication.listed = true;
  const f = fixture(value); f.change(); await f.prepare(); f.setTime(30_100);
  assert.equal(f.source.read().canPromote, false); assert.equal(f.source.read().canRecheckPromote, false);
  f.source.patch({ exposureConfirmed: true }); f.setTime(130_100);
  assert.equal(f.source.read().draft.exposureConfirmed, true); assert.equal(f.source.read().canRecheckPromote, true);
  const calls = [];
  const result = await f.source.recheckPromotion({ isCurrent: f.isCurrent, api: { async request(op, args) {
    calls.push(op); f.setTime(130_300);
    return prepared(args, target({ deviceName: 'Переименованное устройство' }), { preparationId: 'n'.repeat(43), checkedAt: 200_000, expiresAt: 229_900 });
  } } });
  assert.equal(result.status, 'ready'); assert.deepEqual(calls, ['apps.source.prepare']);
  assert.equal(result.intent.args.preparationId, 'n'.repeat(43)); assert.equal(result.intent.args.expectedPolicyEpoch, 8);
  assert.equal(result.intent.args.exposureAck.targetDigest, target().digest); assert.equal(result.intent.args.listed, true);
  assert.equal(result.intent.beforeCreate(), true);
  assert.throws(() => { result.intent.args.launchPolicy = 'restricted'; }, TypeError);
  assert.throws(() => { result.intent.expectedSource.port = 9000; }, TypeError);
});

test('fresh candidate pin drift needs another review and cannot reuse the earlier whole-port consent', async () => {
  for (const changed of [target({ revision: 6 }), target({ digest: 'd'.repeat(64) })]) {
    const value = inspection(); value.publication.launchPolicy = 'anyone'; const f = fixture(value); f.change(); await f.prepare();
    f.setTime(30_100); f.source.patch({ exposureConfirmed: true });
    const result = await f.source.recheckPromotion({ isCurrent: f.isCurrent, api: { async request(_op, args) {
      return prepared(args, changed, { preparationId: 'n'.repeat(43) });
    } } });
    assert.deepEqual(result, { status: 'review' }); assert.equal(f.source.read().draft.exposureConfirmed, false);
    assert.equal(f.source.read().canPromote, false); assert.equal(f.source.read().preparation.target.digest, changed.digest);
  }
});

test('reusing the expired preparation identity fails closed even when its response declares a new lifetime', async () => {
  const f = fixture(); f.change(); await f.prepare(); f.setTime(30_100);
  const result = await f.source.recheckPromotion({ isCurrent: f.isCurrent, api: { request: async (_op, args) => prepared(args) } });
  assert.deepEqual(result, { status: 'review' }); assert.equal(f.source.read().canPromote, false); assert.equal(f.source.read().preparation, null);
});

test('edit ABA, hidden, changed account and explicit consent withdrawal during fresh recheck yield no intent', async () => {
  for (const action of ['aba', 'hidden', 'account', 'consent']) {
    const value = inspection(); value.publication.launchPolicy = 'anyone'; const f = fixture(value); f.change(); await f.prepare();
    f.setTime(30_100); f.source.patch({ exposureConfirmed: true }); const wait = deferred(); let args;
    const rechecking = f.source.recheckPromotion({ isCurrent: f.isCurrent, api: { request(_op, value) { args = value; return wait.promise; } } });
    if (action === 'aba') { f.source.patch({ port: '7000' }); f.source.patch({ port: '6000' }); }
    else if (action === 'hidden') f.source.invalidatePreparation('hidden');
    else if (action === 'account') f.setActive(false);
    else f.source.patch({ exposureConfirmed: false });
    wait.resolve(prepared(args, target(), { preparationId: 'n'.repeat(43) }));
    assert.deepEqual(await rechecking, { status: 'stale' }); assert.equal(f.source.read().canPromote, false);
  }
});

test('revision/publication changes and fresh preparation errors cannot cause an implicit recheck promotion', async () => {
  const f = fixture(); f.change(); await f.prepare(); f.setTime(30_100);
  const wait = deferred(); let args;
  const rechecking = f.source.recheckPromotion({ isCurrent: f.isCurrent, api: { request(_op, value) { args = value; return wait.promise; } } });
  const next = inspection(); next.publication.policyEpoch++; f.source.observe(next);
  wait.resolve(prepared(args, target(), { preparationId: 'n'.repeat(43) }));
  assert.deepEqual(await rechecking, { status: 'stale' }); assert.equal(f.source.read().conflict, true);
  f.source.reset(); f.change(); await f.prepare(); f.setTime(60_100);
  await assert.rejects(f.source.recheckPromotion({ isCurrent: f.isCurrent, api: { request: async () => { throw new Error('offline'); } } }), /offline/u);
  assert.equal(f.source.read().preparation, null); assert.equal(f.source.read().canPromote, false);
});

test('an unsaved publication blocks new recheck but does not change the durable command already stored', async () => {
  const f = fixture(); f.change(); await f.prepare(); const store = storeFixture(); const pending = await store.state.prepare(f.source.promoteIntent());
  f.setTime(30_100); f.source.observe(inspection(), { publicationDirty: true }); let calls = 0;
  await assert.rejects(f.source.recheckPromotion({ isCurrent: f.isCurrent, api: { request() { calls++; } } }), { code: 'app_source_review_required' });
  assert.equal(calls, 0); assert.deepEqual(await store.state.pendingForDispatch(pending), pending);
});

test('retry is pinned to the displayed full command: replacement, removal and same-ID metadata changes dispatch nothing', async () => {
  for (const replace of ['different-command', 'empty', 'same-request-metadata']) {
    const f = fixture(); f.change(); await f.prepare(); const store = storeFixture();
    const first = await store.state.prepare(f.source.promoteIntent()), displayed = copy(first);
    const other = createAppSettingsState({ ...store.options, randomId: () => first.args.requestId });
    await other.abandon(first);
    let latest = null;
    if (replace === 'different-command') latest = await other.prepare({ op: 'apps.domains.claim', args: {
      appId, expectedAccountId: 'owner', expectedDomainsRevision: 1, slug: 'different-project' } });
    else if (replace === 'same-request-metadata') latest = await other.prepare({ ...f.source.promoteIntent(), expectedSource: target({ deviceName: 'Другой показанный источник' }) });
    let calls = 0;
    await assert.rejects(dispatchAppSettingsIntent({ state: store.state, expectedPending: displayed, isCurrent: () => true,
      api: { request() { calls++; throw new Error('must not dispatch'); } } }), { code: 'app_settings_pending_changed' });
    assert.equal(calls, 0); assert.deepEqual(store.state.read().pending, latest);
  }
});

test('retry captures expected metadata before waiting for a local lock; caller mutation cannot retarget it', async () => {
  const f = fixture(); f.change(); await f.prepare(); const store = storeFixture(), intent = f.source.promoteIntent();
  const first = await store.state.prepare(intent), displayed = copy(first), wait = deferred(); let entered;
  const delayed = createAppSettingsState({ ...store.options, locks: { async request(_key, callback) { entered = callback; await wait.promise; return callback(); } } });
  let calls = 0;
  const sending = dispatchAppSettingsIntent({ state: delayed, expectedPending: displayed, isCurrent: () => true,
    api: { request() { calls++; throw new Error('must not dispatch'); } } });
  assert.equal(typeof entered, 'function');
  await store.state.abandon(first); const next = await store.state.prepare({ ...intent, expectedSource: target({ deviceName: 'Changed after click' }) });
  Object.assign(displayed, copy(next)); wait.resolve();
  await assert.rejects(sending, { code: 'app_settings_pending_changed' });
  assert.equal(calls, 0); assert.deepEqual(store.state.read().pending, next);
});

test('a retry without the exact displayed command is rejected instead of silently reading the latest slot', async () => {
  const f = fixture(); f.change(); await f.prepare(); const store = storeFixture();
  const pending = await store.state.prepare(f.source.promoteIntent()); let calls = 0;
  await assert.rejects(dispatchAppSettingsIntent({ state: store.state, isCurrent: () => true,
    api: { request() { calls++; throw new Error('must not dispatch'); } } }), { code: 'app_settings_pending_changed' });
  await assert.rejects(store.state.pendingForDispatch(), { code: 'app_settings_pending_changed' });
  assert.equal(calls, 0); assert.deepEqual(store.state.read().pending, pending);
});


test('authenticated workspace consent persists the bounded entry scope through lost ACK and reload', async () => {
  const snapshot = inspection(), selectedProfile = 'soty.selected-human-embed.v2';
  snapshot.publication.launchPolicy = 'anyone'; snapshot.publication.listed = true;
  snapshot.source.integrations = [{ profile: selectedProfile, entryPath: '/embed' }];
  const f = fixture(snapshot); f.source.patch({ profile: selectedProfile, entryPath: '/embed' });
  await f.prepare(normalizeAppSourceTarget(Object.fromEntries(Object.keys(target()).map(key => [key, ({ ...snapshot.source, revision: 5, digest: 'c'.repeat(64), profile: selectedProfile, entryPath: '/embed' })[key]]))));
  f.source.patch({ exposureConfirmed: true });
  const intent = f.source.promoteIntent(), store = storeFixture();
  assert.equal(intent.args.exposureAck.scope, 'authenticated-workspace'); assert.equal(intent.args.listed, true);
  await assert.rejects(dispatchAppSettingsIntent({ ...intent, state: store.state, isCurrent: () => true,
    api: { request: async () => { throw Error('lost ACK'); } } }));
  const recovered = createAppSettingsState(store.options), pending = recovered.read().pending;
  assert.deepEqual(pending.args.exposureAck, intent.args.exposureAck);
  await assert.rejects(recovered.acknowledge(pending, { ...receipt(pending), receipt: { ...receipt(pending).receipt,
    exposureAck: { ...pending.args.exposureAck, scope: 'whole-port' } } }), { code: 'app_settings_invalid_receipt' });
  assert.equal(await recovered.acknowledge(pending, receipt(pending)), true);
});

// Component-methods-under-ports: execute the actual settings handler, durable
// dispatcher and real Source state. This is not a full mount or browser test.
async function compactSourceMethodsUnderPorts(sourceModel, api, store, quickDetails) {
  const [{ readFile }, { default: ts }, { default: vm }] = await Promise.all([
    import('node:fs/promises'), import('typescript'), import('node:vm'),
  ]);
  const source = await readFile(new URL('./app-settings.ts', import.meta.url), 'utf8');
  const file = ts.createSourceFile('app-settings.ts', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  const mount = file.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'mountAppSettings');
  assert.ok(mount?.body, 'actual mountAppSettings must exist');
  const functions = ['perform', 'promoteSource', 'mutation'].map(name => {
    const matches = mount.body.statements.filter(node => ts.isFunctionDeclaration(node) && node.name?.text === name);
    assert.equal(matches.length, 1, `actual ${name} declaration`); return matches[0].getText(file);
  });
  const declarations = mount.body.statements.filter(ts.isVariableStatement).flatMap(node => [...node.declarationList.declarations]);
  const guards = declarations.filter(node => ts.isIdentifier(node.name) && node.name.text === 'sourceCurrent');
  assert.equal(guards.length, 1, 'actual sourceCurrent declaration');
  let guard = `const ${guards[0].getText(file)};`;
  if (process.env.SOTY_QUICK_SOURCE_OUTER_GUARD === 'off') {
    const term = " && selectedSection === 'hosting'";
    assert.equal(guard.split(term).length, 2, 'negative control removes only the actual outer disclosure term');
    guard = guard.replace(term, '');
  }
  const code = `module.exports = ports => {
    const { sourceModel, api, store, quickDetails, dispatchAppSettingsIntent } = ports;
    const document = { visibilityState: 'visible' }, advanced = { open: true }, sourceDetails = { open: true };
    const current = () => true, sourcePromote = {}, sourceHistory = { open: false }, model = null;
    const disabled = () => false, render = () => {}, settingsError = reason => String(reason?.code ?? reason);
    let selectedSection = 'hosting', busy = false, refreshing = false, refreshGeneration = 0, inspectionGeneration = 0, copyGeneration = 0;
    let error = '', copyFallback = null, sourcePrimaryAction = 'recheck', sourceGeneration = 0;
    let sourceError = '', sourceNotice = '', sourceRechecking = false, hadPreparation = true;
    let staleInspection = false, sourceReadbackNotice = null, notice = '', readbacks = 0;
    const inspect = async () => { readbacks++; return true; };
    const loadSourceHistory = () => { throw new Error('unexpected history request'); };
    ${guard}
    ${functions.join('\n')}
    return { invoke: promoteSource, leaveHosting() { selectedSection = 'about'; }, parents: { advanced, sourceDetails },
      read: () => ({ sourceError, error, readbacks, busy, sourceNotice }) };
  };`;
  const compiled = ts.transpileModule(code, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
  const module = { exports: {} };
  vm.runInNewContext(compiled, { module, exports: module.exports });
  return module.exports({ sourceModel, api, store, quickDetails, dispatchAppSettingsIntent });
}

async function sourceDisclosureCase(quickDetails) {
  const f = fixture(), saved = storeFixture(), gate = deferred(), calls = []; let heldArgs;
  f.change(); await f.prepare(); f.setTime(30_100);
  assert.equal(f.source.read().canRecheckPromote, true, 'a real expired reviewed candidate admits the explicit recheck action');
  const api = { request(op, args) {
    calls.push({ op, args: copy(args) });
    if (op === 'apps.source.prepare') { heldArgs = args; return gate.promise; }
    assert.equal(op, 'apps.source.promote', 'only the actual durable source effect may follow recheck');
    return Promise.resolve(receipt(saved.state.read().pending));
  } };
  const methods = await compactSourceMethodsUnderPorts(f.source, api, saved.state, quickDetails);
  const invoking = methods.invoke();
  assert.equal(calls.length, 1); assert.equal(calls[0].op, 'apps.source.prepare');
  if (quickDetails) { quickDetails.open = false; methods.leaveHosting(); }
  // Native toggle is a later task. Nested disclosures deliberately stay open
  // while the held promise resolves, so their toggle handlers cannot hide a bug.
  assert.equal(methods.parents.advanced.open, true); assert.equal(methods.parents.sourceDetails.open, true);
  const beforeRelease = calls.length;
  f.setTime(30_300); gate.resolve(prepared(heldArgs, target(), { preparationId: 'n'.repeat(43), checkedAt: 200_000, expiresAt: 229_900 }));
  await invoking;
  return { f, saved, methods, calls, afterRelease: calls.slice(beforeRelease) };
}

test('component methods: leaving the Hosting section fences a held Source recheck without a new prepare, promotion or journal', async () => {
  const result = await sourceDisclosureCase({ open: true });
  assert.deepEqual(result.afterRelease, [], 'closed outer disclosure must suppress the actual handler continuation');
  assert.equal(result.calls.filter(value => value.op === 'apps.source.prepare').length, 1, 'only the already-held preparation was sent');
  assert.equal(result.calls.filter(value => value.op === 'apps.source.promote').length, 0);
  assert.equal(result.saved.state.read().pending, null); assert.equal(result.saved.values.size, 0);
  assert.equal(result.methods.read().readbacks, 0); assert.equal(result.methods.read().sourceError, '');
  assert.equal(result.methods.read().busy, false); assert.equal(result.f.source.read().preparation, null);
});

test('component methods: remaining in the Hosting section rechecks and uses its existing durable Source dispatcher', async () => {
  const result = await sourceDisclosureCase(null);
  assert.deepEqual(result.calls.map(value => value.op), ['apps.source.prepare', 'apps.source.promote']);
  assert.equal(result.methods.read().readbacks, 1); assert.equal(result.methods.read().sourceError, '');
  assert.equal(result.methods.read().busy, false); assert.equal(result.saved.state.read().pending, null);
  assert.equal(result.afterRelease[0].args.preparationId, 'n'.repeat(43));
});
