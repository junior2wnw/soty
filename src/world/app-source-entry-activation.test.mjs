import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createAppSourceState } from './app-source-state.mjs';
import { createAppSettingsState, dispatchAppSettingsIntent } from './app-settings-state.mjs';

const appId = `app-${'1'.repeat(32)}`, nativeId = `dom_${'a'.repeat(32)}`, sourceId = `dom_${'b'.repeat(32)}`;
const nativeOrigin = 'https://native.apps.test', sourceOrigin = 'https://source.apps.test';
const profile = 'soty.selected-human-embed.v2', clone = value => structuredClone(value);
const sha = value => createHash('sha256').update(value).digest('hex');
function fixture({ capability = true, extraAlias = false, sourceOnly = false, zeroActive = false, activeSource = false } = {}) {
  let time = 10;
  const native = { revision: 1, digest: 'a'.repeat(64), profile: 'soty.relay-restricted.v1', hostDeviceId: 'host', connectorId: 'connector', deviceName: 'Computer', port: 5000, entryPath: '/' };
  const next = { ...native, revision: 2, digest: 'b'.repeat(64), profile, port: 6000, entryPath: '/embed' };
  const entry = { origin: sourceOrigin, domainId: sourceId }, pair = { origin: nativeOrigin, domainId: nativeId, target: native };
  const snapshot = { app: { id: appId, state: 'enabled' },
    publication: { policyEpoch: 3, launchPolicy: 'anyone', listed: !zeroActive, activeDomainIds: zeroActive ? [] : [nativeId, ...(activeSource ? [sourceId] : [])], activeTargetRevision: 1 },
    addresses: { aliases: [{ id: nativeId, origin: nativeOrigin, state: 'bound', active: !zeroActive }, { id: sourceId, origin: sourceOrigin, state: 'bound', active: activeSource },
      ...(extraAlias ? [{ id: `dom_${'c'.repeat(32)}`, origin: 'https://other.apps.test', state: 'bound', active: false }] : [])] },
    source: { ...native, ...(sourceOnly ? { profile, entryPath: '/embed' } : {}), sourceEntryActivation: capability,
      requiredBindingVersion: 2, integrations: [{ profile, entryPath: '/embed' }] }, actions: { canEdit: true, canPublish: true } };
  const state = createAppSourceState({ accountId: 'owner', appId, snapshot, now: () => time });
  state.patch({ profile, port: '6000', entryPath: '/embed', ...(sourceOnly ? { sourceDomainId: sourceId } : {}) });
  const response = (args, overrides = {}) => ({ schema: 'soty.app-source-preparation.v1', appId, preparationId: 'p'.repeat(43),
    expectedPolicyEpoch: 3, expectedTargetRevision: 1, requiredBindingVersion: 2, target: next, checkedAt: 100_000, expiresAt: 130_000,
    ...(args.nativeEntry ? { nativeEntry: pair } : {}),
    ...(args.sourceEntryAction ? { sourceEntryAction: clone(args.sourceEntryAction), sourceEntry: entry } : {}), ...overrides });
  return { state, snapshot, entry, pair, next, response, setTime(value) { time = value; },
    prepare(overrides) { return state.prepare({ isCurrent: () => true, api: { request: async (_op, args) => response(args, overrides) } }); },
    consent() { state.patch({ exposureConfirmed: true, ...(sourceOnly ? {} : { nativeConfirmed: true }) }); } };
}
function store() {
  const values = new Map(); const options = { accountId: 'owner', appId, storage: { getItem: key => values.get(key) ?? null, setItem: (key, value) => values.set(key, value) },
    locks: { request: async (_key, callback) => callback() }, randomId: () => 'atomic-source-action' };
  return { values, options, state: createAppSettingsState(options) };
}
function receipt(pending) {
  const args = pending.args, target = pending.expectedSource;
  return { requestId: args.requestId, replayed: false, receipt: { schema: 'soty.app-source-receipt.v2', namespace: 'apps.source.promote.v2', appId,
    requestKeyHash: sha(args.requestId), preparationKeyHash: sha(args.preparationId), previousTargetRevision: args.expectedTargetRevision,
    policyEpoch: args.expectedPolicyEpoch + 1, targetRevision: target.revision, targetDigest: target.digest, profile: target.profile,
    requiredBindingVersion: 2, launchPolicy: args.launchPolicy, listed: args.listed, exposureAck: args.exposureAck, committedAt: 100_010,
    sourceEntryAction: args.sourceEntryAction, sourceEntry: pending.expectedSourceEntry,
    ...(args.nativeExposureAck ? { nativeEntry: { ...pending.expectedNativeEntry, target: { revision: args.nativeExposureAck.targetRevision,
      digest: args.nativeExposureAck.targetDigest, profile: args.nativeExposureAck.profile }, exposureAck: args.nativeExposureAck } } : {}) },
    current: { appId, policyEpoch: args.expectedPolicyEpoch + 1, activeTargetRevision: target.revision, requiredBindingVersion: 2 } };
}

test('inactive Source name is reviewed privately and joins the exact existing Source/Native consent intent', async () => {
  const f = fixture(); assert.equal(f.state.read().draft.sourceDomainId, sourceId);
  assert.equal(f.state.read().sourceEntry.origin, sourceOrigin); assert.equal(f.snapshot.addresses.aliases[1].active, false);
  await f.state.prepare({ isCurrent: () => true, api: { request: async (_op, args) => {
    assert.deepEqual(args.sourceEntryAction, { action: 'activate', domainId: sourceId });
    assert.equal(Object.hasOwn(args.sourceEntryAction, 'origin'), false); return f.response(args);
  } } });
  assert.equal(f.snapshot.addresses.aliases[1].active, false, 'prepare never activates the alias');
  f.state.patch({ exposureConfirmed: true }); assert.equal(f.state.read().canPromote, false);
  f.state.patch({ nativeConfirmed: true }); const intent = f.state.promoteIntent();
  assert.deepEqual(intent.args.sourceEntryAction, { action: 'activate', domainId: sourceId });
  assert.deepEqual(intent.expectedSourceEntry, f.entry); assert.equal(intent.args.exposureAck.scope, 'authenticated-workspace');
  assert.equal(intent.args.nativeExposureAck.scope, 'whole-port'); assert.equal(intent.beforeCreate(), true);
});

test('missing capability blocks inactive-alias migration before any request and never publishes the ordinary project', async () => {
  for (const capability of [false, undefined]) {
    const f = fixture({ capability }); if (capability === undefined) { delete f.snapshot.source.sourceEntryActivation; f.state.observe(f.snapshot); }
    let calls = 0; assert.equal(f.state.read().canPrepare, false);
    await assert.rejects(f.state.prepare({ isCurrent: () => true, api: { request() { calls++; } } }), { code: 'app_source_entry_activation_unsupported' });
    assert.equal(calls, 0); assert.deepEqual(f.snapshot.publication.activeDomainIds, [nativeId]);
  }
});

test('ambiguous inactive names require selection; malformed or redirected action projections are rejected', async () => {
  const f = fixture({ extraAlias: true }); assert.equal(f.state.read().draft.sourceDomainId, null);
  f.state.patch({ sourceDomainId: sourceId });
  for (const overrides of [
    { sourceEntryAction: { action: 'activate', domainId: nativeId } },
    { sourceEntryAction: { action: 'activate', domainId: sourceId, origin: sourceOrigin } },
    { sourceEntry: { origin: 'https://other.apps.test', domainId: sourceId } },
    { sourceEntry: { ...f.entry, unreviewed: true } },
  ]) await assert.rejects(f.prepare(overrides), { code: 'app_source_invalid_preparation' });
  await f.prepare(); f.consent(); const intent = f.state.promoteIntent();
  f.state.patch({ sourceDomainId: `dom_${'c'.repeat(32)}` });
  assert.throws(() => intent.beforeCreate(), { code: 'apps_source_preparation_expired' });
});

test('an alias change or capability downgrade invalidates the check and independent ACKs without rebasing an unsent candidate', async () => {
  for (const change of [value => { value.addresses.aliases[1].state = 'tombstone'; }, value => { value.source.sourceEntryActivation = false; }]) {
    const f = fixture(); await f.prepare(); f.consent(); const value = clone(f.snapshot); change(value); f.state.observe(value);
    const state = f.state.read(); assert.equal(state.preparation, null); assert.equal(state.draft.exposureConfirmed, false);
    assert.equal(state.draft.nativeConfirmed, false); assert.equal(state.canPromote, false); assert.equal(state.draft.port, '6000');
  }
});

test('expired check retains Source activation identity and cannot reuse consent for a changed origin or action', async () => {
  const f = fixture(); await f.prepare(); f.consent(); f.setTime(40_000);
  assert.equal(f.state.read().canRecheckPromote, true);
  await assert.rejects(f.state.recheckPromotion({ isCurrent: () => true, api: { request: async (_op, args) => f.response(args,
    { preparationId: 'q'.repeat(43), sourceEntry: { origin: 'https://other.apps.test', domainId: sourceId } }) } }), { code: 'app_source_invalid_preparation' });
  assert.equal(f.state.read().canPromote, false);
});

test('Source activation survives lost ACK and reload; a forged origin/action cannot clear or replace the exact pending command', async () => {
  const f = fixture(); await f.prepare(); f.consent(); const intent = f.state.promoteIntent(), local = store();
  await assert.rejects(dispatchAppSettingsIntent({ ...intent, state: local.state, isCurrent: () => true, api: { request: async () => { throw Error('lost'); } } }));
  const pending = local.state.read().pending, remounted = createAppSettingsState(local.options);
  assert.deepEqual(remounted.read().pending.expectedSourceEntry, f.entry);
  for (const mutate of [r => { r.receipt.sourceEntry.origin = 'https://other.apps.test'; }, r => { r.receipt.sourceEntryAction.domainId = nativeId; },
    r => { r.receipt.sourceEntryAction.extra = true; }, r => { r.receipt.sourceEntry.extra = true; }]) {
    const bad = clone(receipt(pending)); mutate(bad); await assert.rejects(remounted.acknowledge(pending, bad), { code: 'app_settings_invalid_receipt' });
    assert.deepEqual(remounted.read().pending, pending);
  }
  let sent; const result = await dispatchAppSettingsIntent({ state: remounted, expectedPending: pending, isCurrent: () => true,
    api: { request: async (_op, args) => { sent = args; const result = clone(receipt(pending)); result.replayed = true; result.current.policyEpoch += 2;
      result.receipt.sourceEntryAction = { domainId: sourceId, action: 'activate' }; result.receipt.sourceEntry = { domainId: sourceId, origin: sourceOrigin }; return result; } } });
  assert.deepEqual(sent, pending.args); assert.equal(result.status, 'accepted'); assert.equal(remounted.read().pending, null);
});

test('Source-only activation uses v2 without fabricating a Native ACK; invalid activation policy fails before persistence', async () => {
  const f = fixture({ sourceOnly: true }); await f.prepare(); f.consent(); const intent = f.state.promoteIntent(), local = store();
  assert.equal(intent.args.nativeExposureAck, undefined); const pending = await local.state.prepare(intent);
  assert.equal(await local.state.acknowledge(pending, receipt(pending)), true);
  for (const mutate of [value => { value.args.sourceEntryAction.origin = sourceOrigin; }, value => { value.args.launchPolicy = 'restricted'; delete value.args.exposureAck; },
    value => { value.expectedSourceEntry.domainId = nativeId; }, value => { value.args.exposureAck.scope = 'whole-port'; }]) {
    const value = clone({ ...intent, beforeCreate: undefined }); mutate(value);
    await assert.rejects(local.state.prepare({ ...value, beforeCreate: intent.beforeCreate })); assert.equal(local.state.read().pending, null);
  }
});

test('the first Source-only public name can be activated from zero active aliases after its verified workspace ACK', async () => {
  const f = fixture({ sourceOnly: true, zeroActive: true });
  assert.equal(f.state.read().snapshot.publication.activeDomainIds.length, 0);
  await f.prepare(); assert.equal(f.state.read().canPromote, false); f.consent();
  assert.equal(f.state.read().canPromote, true);
  const intent = f.state.promoteIntent(), local = store(); assert.equal(intent.args.nativeExposureAck, undefined);
  const pending = await local.state.prepare(intent); assert.equal(await local.state.acknowledge(pending, receipt(pending)), true);
});

test('old-server no-action migration remains valid with an active Source alias and an unrelated inactive alias', async () => {
  const f = fixture({ capability: false, activeSource: true, extraAlias: true });
  f.state.patch({ sourceDomainId: null, nativeDomainId: nativeId });
  assert.equal(f.state.read().canPrepare, true); assert.equal(f.state.read().sourceEntryActivationUnsupported, false);
  await f.state.prepare({ isCurrent: () => true, api: { request: async (_op, args) => {
    assert.equal(Object.hasOwn(args, 'sourceEntryAction'), false); return f.response(args);
  } } });
  f.consent(); const intent = f.state.promoteIntent();
  assert.equal(Object.hasOwn(intent.args, 'sourceEntryAction'), false); assert.equal(intent.expectedSourceEntry, undefined);
});
