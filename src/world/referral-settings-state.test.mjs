import test from 'node:test';
import assert from 'node:assert/strict';
import { createReferralSettingsState, dispatchReferralSettingsIntent, referralPercentToBps, referralBpsToPercent } from './referral-settings-state.mjs';

const accountId = 'referral-settings-owner', appId = 'app-' + 'a'.repeat(32);
const rounding = 'floor-per-beneficiary;cumulative-refund-of-original-commission';
const codeIs = code => error => { assert.equal(error.code, code); return true; };
const result = (revision = 1, ratesBps = [2000, 500, 500]) => ({ appId, revision, ratesBps, rounding, payoutsEnabled: false, updatedAt: 1 });
function local() {
  const values = new Map(), queues = new Map(); let serial = 0, mode = 'normal';
  const storage = {
    getItem: key => values.get(key) ?? null,
    setItem(key, value) { if (mode === 'throw') throw new Error('quota'); if (mode !== 'ignore') values.set(key, value); },
  };
  const locks = { request(key, action) {
    const next = (queues.get(key) ?? Promise.resolve()).catch(() => {}).then(action);
    queues.set(key, next.catch(() => {})); return next;
  } };
  return { values, storage, locks, mode: value => { mode = value; },
    state: (options = {}) => createReferralSettingsState({ accountId, appId, storage, locks, randomId: () => `settings-${++serial}`, ...options }) };
}
const prepare = state => state.prepare({ expectedRevision: 0, ratesBps: [2000, 500, 500] });
const rejectionFor = pending => ({ code: 'referrals_revision_conflict', mutationId: pending.mutationId,
  appId: pending.appId, expectedAccountId: pending.expectedAccountId, expectedRevision: pending.expectedRevision });

test('referral percentages parse decimal strings exactly, reject invalid precision, and roundtrip every supported basis point', () => {
  assert.equal(referralPercentToBps('10'), 1000); assert.equal(referralPercentToBps('0,25'), 25);
  assert.equal(referralPercentToBps(' 0.25 '), 25); assert.equal(referralPercentToBps('100.00'), 10000);
  for (const bad of ['', ' ', '-0', '-1', '+10', '.5', '1e1', 'NaN', 'Infinity', '10%', '100,01', '0.001', '1,2.3', null, 10]) {
    assert.throws(() => referralPercentToBps(bad), codeIs('referral_settings_invalid_percent'));
  }
  for (let bps = 0; bps <= 10000; bps++) assert.equal(referralPercentToBps(referralBpsToPercent(bps)), bps);
});

test('concurrent tabs reuse one durable intent; accounts and applications have independent queues', async () => {
  const f = local(), one = f.state(), two = f.state();
  const [a, b] = await Promise.all([prepare(one), prepare(two)]); assert.deepEqual(a, b);
  assert.deepEqual(f.state().read().pending, a);
  assert.equal(f.state({ accountId: 'another-account' }).read().pending, null);
  assert.equal(f.state({ appId: 'app-' + 'b'.repeat(32) }).read().pending, null);
  a.ratesBps[0] = 9999; assert.deepEqual(one.read().pending.ratesBps, [2000, 500, 500]);
  await assert.rejects(one.prepare({ expectedRevision: 1, ratesBps: [1000] }), codeIs('referral_settings_pending_unconfirmed'));
});

test('invalid rates, denied or silently ignored storage, corruption, and missing locks cannot start a write', async () => {
  for (const ratesBps of [[], Array(11).fill(0), [-0], [0.25], [-1], [10001], [5001, 5000]]) {
    const f = local(); await assert.rejects(f.state().prepare({ expectedRevision: 0, ratesBps })); assert.equal(f.values.size, 0);
  }
  for (const mode of ['throw', 'ignore']) {
    const f = local(); f.mode(mode); await assert.rejects(prepare(f.state()), codeIs('referral_settings_storage_unavailable')); assert.equal(f.values.size, 0);
  }
  const f = local(), state = f.state(); await assert.rejects(prepare(f.state({ locks: null })), codeIs('referral_settings_lock_unavailable'));
  f.values.set(state.key, '{corrupt'); assert.throws(() => state.read(), codeIs('referral_settings_storage_unavailable'));
});

test('a pending command copied from another account and a stale displayed command cannot be dispatched', async () => {
  const f = local(), state = f.state(), pending = await prepare(state);
  await assert.rejects(state.pendingForDispatch({ ...pending, mutationId: 'not-the-displayed-command' }), codeIs('referral_settings_pending_changed'));
  const stored = JSON.parse(f.values.get(state.key)); stored.pending.expectedAccountId = 'other'; f.values.set(state.key, JSON.stringify(stored));
  assert.throws(() => state.read(), codeIs('referral_settings_storage_unavailable'));
});

test('malformed receipts never retire the durable command or claim a saved configuration', async () => {
  for (const receipt of [result(2), result(1, [1000]), { ...result(), appId: 'wrong-app' }, { ...result(), payoutsEnabled: true }, { ...result(), rounding: 'nearest' }]) {
    const f = local(), state = f.state(), pending = await prepare(state); let calls = 0;
    await assert.rejects(dispatchReferralSettingsIntent({ state, pending, isCurrent: () => true, request: async () => { calls++; return receipt; } }));
    assert.equal(calls, 1); assert.deepEqual(state.read().pending, pending);
  }
});

test('a failed readback preserves the mutation and a replay reads the newer head instead of displaying its old receipt', async () => {
  const f = local(), state = f.state(), pending = await prepare(state), ids = [];
  await assert.rejects(dispatchReferralSettingsIntent({ state, pending, isCurrent: () => true, request: async (op, args) => {
    if (op === 'referrals.config.put') { ids.push(args.mutationId); return result(); } throw new Error('readback offline');
  } }), /readback offline/u);
  assert.deepEqual(state.read().pending, pending);
  const reopened = f.state();
  const head = await dispatchReferralSettingsIntent({ state: reopened, pending: reopened.read().pending, isCurrent: () => true, request: async (op, args) => {
    if (op === 'referrals.config.put') { ids.push(args.mutationId); return { ...result(), replayed: true }; } return result(2, [2500, 250, 250]);
  } });
  assert.deepEqual(ids, [pending.mutationId, pending.mutationId]); assert.equal(head.revision, 2);
  assert.deepEqual(head.ratesBps, [2500, 250, 250]); assert.equal(f.state().read().pending, null);
});

test('older or contradictory readback leaves the pending command recoverable', async () => {
  for (const head of [result(0), result(1, [1000])]) {
    const f = local(), state = f.state(), pending = await prepare(state);
    await assert.rejects(dispatchReferralSettingsIntent({ state, pending, isCurrent: () => true,
      request: async op => op === 'referrals.config.put' ? result() : head }), codeIs('referral_settings_invalid_receipt'));
    assert.deepEqual(state.read().pending, pending);
  }
});

test('a late acknowledgement never deletes another tabs newer mutation', async () => {
  const f = local(), state = f.state(), old = await prepare(state);
  await state.acknowledge(old, result());
  const next = await state.prepare({ expectedRevision: 1, ratesBps: [1000] });
  await assert.rejects(state.acknowledge(old, result()), codeIs('referral_settings_pending_changed'));
  assert.deepEqual(state.read().pending, next);
});

test('two tabs may acknowledge the same committed mutation without manufacturing an unresolved conflict', async () => {
  const f = local(), state = f.state(), pending = await prepare(state);
  const values = await Promise.all([state.acknowledge(pending, result()), f.state().acknowledge(pending, result())]);
  assert.deepEqual(values, [true, false]); assert.equal(state.read().pending, null);
});

test('account changes before dispatch, after a server reply, and while waiting for acknowledgement prevent stale success', async () => {
  const f = local(), state = f.state(), pending = await prepare(state); let calls = 0, active = false;
  const request = async () => { calls++; active = false; return result(); };
  await assert.rejects(dispatchReferralSettingsIntent({ state, pending, request, isCurrent: () => active }), codeIs('referral_settings_account_changed'));
  assert.equal(calls, 0); active = true;
  await assert.rejects(dispatchReferralSettingsIntent({ state, pending, request, isCurrent: () => active }), codeIs('referral_settings_account_changed'));
  assert.equal(calls, 1); assert.deepEqual(state.read().pending, pending);
  await assert.rejects(state.acknowledge(pending, result(), () => false), codeIs('referral_settings_account_changed'));
  assert.deepEqual(state.read().pending, pending);
});

test('only a confirmed CAS rejection allows explicit atomic replacement or taking current settings', async () => {
  const f = local(), state = f.state(), original = await prepare(state), rejection = rejectionFor(original);
  for (const reason of [{ code: 'network_error' }, { code: 'referrals_forbidden' }, null]) {
    await assert.rejects(state.resolveRejected(original, reason, null, () => true), codeIs('referral_settings_rejection_unconfirmed'));
    assert.deepEqual(state.read().pending, original);
  }
  const replacement = await state.resolveRejected(original, rejection, { expectedRevision: 1, ratesBps: [1000] }, () => true);
  assert.notEqual(replacement.mutationId, original.mutationId); assert.equal(replacement.expectedRevision, 1);
  assert.deepEqual(f.state().read().pending, replacement);
  await assert.rejects(state.acknowledge(original, result()), codeIs('referral_settings_pending_changed'));
  assert.equal(await state.resolveRejected(replacement, rejectionFor(replacement), null, () => true), null);
  assert.equal(state.read().pending, null);
});

test('failed conflict resolution preserves the original intent rather than dropping it before replacement', async () => {
  const f = local(), state = f.state(), pending = await prepare(state), rejection = rejectionFor(pending);
  await assert.rejects(state.resolveRejected(pending, rejection, { expectedRevision: 0, ratesBps: [1000] }, () => true));
  await assert.rejects(state.resolveRejected(pending, rejection, null, () => false), codeIs('referral_settings_account_changed'));
  f.mode('ignore');
  await assert.rejects(state.resolveRejected(pending, rejection, { expectedRevision: 1, ratesBps: [1000] }, () => true), codeIs('referral_settings_storage_unavailable'));
  assert.deepEqual(state.read().pending, pending);
});

test('a rejection of the previous mutation cannot discard its replacement after that replacement loses its ACK', async () => {
  const f = local(), state = f.state(), original = await prepare(state), rejection = rejectionFor(original);
  const replacement = await state.resolveRejected(original, rejection, { expectedRevision: 1, ratesBps: [1000] }, () => true);
  await assert.rejects(dispatchReferralSettingsIntent({ state, pending: replacement, isCurrent: () => true,
    request: async () => { throw new Error('connection lost after replacement COMMIT'); } }), /connection lost/u);
  await assert.rejects(state.resolveRejected(replacement, rejection, null, () => true), codeIs('referral_settings_rejection_unconfirmed'));
  assert.deepEqual(state.read().pending, replacement);
  const head = await dispatchReferralSettingsIntent({ state, pending: replacement, isCurrent: () => true,
    request: async () => result(2, [1000]) });
  assert.equal(head.revision, 2); assert.equal(state.read().pending, null);
});
