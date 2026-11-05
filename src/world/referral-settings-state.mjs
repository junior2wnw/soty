const clone = value => structuredClone(value);
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const fail = code => { throw Object.assign(new Error(code), { code }); };
const check = (value, code = 'referral_settings_invalid_intent') => { if (!value) fail(code); };
const integer = value => Number.isSafeInteger(value) && value >= 0 && !Object.is(value, -0);
const id = value => typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$/u.test(value) && !value.includes('..') && !value.includes('//');
const rounding = 'floor-per-beneficiary;cumulative-refund-of-original-commission';

export function referralPercentToBps(text) {
  check(typeof text === 'string' && /^\d{1,3}(?:[.,]\d{1,2})?$/u.test(text.trim()), 'referral_settings_invalid_percent');
  const [whole, fraction = ''] = text.trim().replace(',', '.').split('.');
  const value = Number(whole) * 100 + Number(fraction.padEnd(2, '0'));
  check(integer(value) && value <= 10000, 'referral_settings_invalid_percent');
  return value;
}
export function referralBpsToPercent(value) {
  check(integer(value) && value <= 10000, 'referral_settings_invalid_percent');
  const fraction = String(value % 100).padStart(2, '0').replace(/0+$/u, '');
  return String(Math.floor(value / 100)) + (fraction ? ',' + fraction : '');
}
function rates(value) {
  check(Array.isArray(value) && value.length >= 1 && value.length <= 10
    && value.every(item => integer(item) && item <= 10000)
    && value.reduce((sum, item) => sum + item, 0) <= 10000);
  return [...value];
}
function command(value) {
  check(value && typeof value === 'object' && !Array.isArray(value)
    && Object.keys(value).sort().join(',') === 'appId,expectedAccountId,expectedRevision,mutationId,ratesBps'
    && id(value.expectedAccountId) && /^app-[a-f0-9]{32}$/u.test(value.appId)
    && id(value.mutationId) && integer(value.expectedRevision));
  return { expectedAccountId: value.expectedAccountId, appId: value.appId, mutationId: value.mutationId,
    expectedRevision: value.expectedRevision, ratesBps: rates(value.ratesBps) };
}
function config(value, appId) {
  check(value && value.appId === appId && integer(value.revision) && integer(value.updatedAt)
    && value.rounding === rounding && value.payoutsEnabled === false, 'referral_settings_invalid_receipt');
  rates(value.ratesBps);
  return clone(value);
}
export { config as validateReferralSettingsConfig };
function receipt(value, pending) {
  config(value, pending.appId);
  check(value.revision === pending.expectedRevision + 1 && same(value.ratesBps, pending.ratesBps)
    && (value.replayed === undefined || typeof value.replayed === 'boolean'), 'referral_settings_invalid_receipt');
}

/** The lock protects local transitions; identical network retries are deduplicated by the server. */
export function createReferralSettingsState({ accountId, appId, storage, locks, randomId = () => crypto.randomUUID() }) {
  check(id(accountId) && typeof appId === 'string' && /^app-[a-f0-9]{32}$/u.test(appId), 'referral_settings_invalid_scope');
  const key = `soty.referral-settings.v1:${accountId}:${appId}`;
  const fresh = () => ({ schema: 1, accountId, appId, revision: 0, pending: null });
  function load() {
    try {
      const raw = storage.getItem(key); if (raw === null) return fresh();
      check(typeof raw === 'string' && raw.length <= 16000);
      const value = JSON.parse(raw);
      check(value?.schema === 1 && value.accountId === accountId && value.appId === appId && integer(value.revision)
        && Object.keys(value).sort().join(',') === 'accountId,appId,pending,revision,schema');
      const pending = value.pending === null ? null : command(value.pending);
      check(!pending || pending.expectedAccountId === accountId && pending.appId === appId);
      return { ...fresh(), revision: value.revision, pending };
    } catch { fail('referral_settings_storage_unavailable'); }
  }
  function write(value) {
    try {
      check(integer(value.revision));
      const raw = JSON.stringify(value); storage.setItem(key, raw);
      check(storage.getItem(key) === raw);
    } catch { fail('referral_settings_storage_unavailable'); }
  }
  const locked = action => locks?.request ? locks.request(key, action)
    : Promise.reject(Object.assign(new Error('referral_settings_lock_unavailable'), { code: 'referral_settings_lock_unavailable' }));
  return {
    key,
    read: () => clone(load()),
    async prepare({ expectedRevision, ratesBps }) {
      check(integer(expectedRevision)); const nextRates = rates(ratesBps);
      return locked(() => {
        const value = load();
        if (value.pending) {
          check(value.pending.expectedRevision === expectedRevision && same(value.pending.ratesBps, nextRates), 'referral_settings_pending_unconfirmed');
          return clone(value.pending);
        }
        value.pending = command({ expectedAccountId: accountId, appId, mutationId: randomId(), expectedRevision, ratesBps: nextRates });
        value.revision++; write(value); return clone(value.pending);
      });
    },
    async pendingForDispatch(displayed) {
      const expected = command(displayed);
      return locked(() => {
        const value = load();
        check(same(value.pending, expected), 'referral_settings_pending_changed');
        return clone(value.pending); // Dispatch the exact durable mutation, including its identity.
      });
    },
    async acknowledge(displayed, result, isCurrent = () => true) {
      const expected = command(displayed); receipt(result, expected);
      return locked(() => {
        check(isCurrent() === true, 'referral_settings_account_changed');
        const value = load();
        if (value.pending === null) return false;
        check(same(value.pending, expected), 'referral_settings_pending_changed');
        value.pending = null; value.revision++; write(value); return true;
      });
    },
    async resolveRejected(displayed, rejection, replacement, isCurrent) {
      const expected = command(displayed);
      // The server checks replay before CAS: this exact conflict proves the mutation did not commit.
      check(rejection?.code === 'referrals_revision_conflict' && rejection.mutationId === expected.mutationId
        && rejection.appId === expected.appId && rejection.expectedAccountId === expected.expectedAccountId
        && rejection.expectedRevision === expected.expectedRevision, 'referral_settings_rejection_unconfirmed');
      const next = replacement === null ? null : { expectedRevision: replacement.expectedRevision, ratesBps: rates(replacement.ratesBps) };
      check(next === null || integer(next.expectedRevision) && next.expectedRevision > expected.expectedRevision);
      return locked(() => {
        check(typeof isCurrent === 'function' && isCurrent() === true, 'referral_settings_account_changed');
        const value = load(); check(same(value.pending, expected), 'referral_settings_pending_changed');
        value.pending = next === null ? null : command({ expectedAccountId: accountId, appId, mutationId: randomId(), ...next });
        value.revision++; write(value); return clone(value.pending);
      });
    },
  };
}

export async function dispatchReferralSettingsIntent({ state, pending, request, isCurrent }) {
  const current = () => check(typeof isCurrent === 'function' && isCurrent() === true, 'referral_settings_account_changed');
  current();
  const args = await state.pendingForDispatch(pending); current();
  let result;
  try { result = await request('referrals.config.put', clone(args)); }
  catch (reason) {
    if (reason?.code === 'referrals_revision_conflict') throw Object.assign(new Error(reason.code), {
      code: reason.code, mutationId: args.mutationId, appId: args.appId,
      expectedAccountId: args.expectedAccountId, expectedRevision: args.expectedRevision,
    });
    throw reason;
  }
  current(); receipt(result, args);
  // A replay receipt can precede a newer owner's configuration; it is not the current head.
  const head = config(await request('referrals.config.get', { expectedAccountId: args.expectedAccountId, appId: args.appId }), args.appId);
  current();
  check(head.revision >= result.revision && (head.revision !== result.revision || same(head.ratesBps, result.ratesBps)), 'referral_settings_invalid_receipt');
  await state.acknowledge(args, result, isCurrent); current();
  return head;
}
