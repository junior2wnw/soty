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
  return { requestId: a.requestId, replayed: false, receipt: { schema: 'soty.app-source-receipt.v1', namespace: 'apps.source.promote.v1',
    appId, requestKeyHash: sha(a.requestId), preparationKeyHash: sha(a.preparationId), previousTargetRevision: a.expectedTargetRevision,
    targetRevision: t.revision, targetDigest: t.digest, profile: t.profile, requiredBindingVersion: 2, policyEpoch: a.expectedPolicyEpoch + 1,
    launchPolicy: a.launchPolicy, listed: a.listed, exposureAck: a.exposureAck ?? null, committedAt: 101_000 },
    current: { appId, policyEpoch: a.expectedPolicyEpoch + 1, requiredBindingVersion: 2, activeTargetRevision: t.revision } };
}

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
