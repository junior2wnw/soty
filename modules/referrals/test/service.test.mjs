import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createReferralsService, REFERRAL_OPERATIONS } from '../server/index.mjs';

const person = accountId => Object.freeze({ accountId, deviceId: `device-${accountId}` });
const owner = person('account-owner'), otherOwner = person('account-other-owner');
const payer = person('account-payer'), one = person('account-one'), two = person('account-two'), three = person('account-three'), four = person('account-four');
const owners = { 'app-a': owner.accountId, 'app-b': otherOwner.accountId };
const members = { 'app-a': [owner, payer, one, two, three, four].map(a => a.accountId), 'app-b': [otherOwner, payer, one, two, three, four].map(a => a.accountId) };
const fail = (fn, code) => assert.throws(fn, error => error.code === code);
let serial = 0;

function fixture(t, overrides = {}) {
  const base = resolve(tmpdir()), directory = mkdtempSync(join(base, 'soty-referrals-test-')), databasePath = join(directory, 'referrals.sqlite');
  let time = 100, service; const probes = [];
  const proofs = new WeakMap();
  const options = { databasePath, projectId: 'referrals-test', clock: () => time++, authorize: ({ actor, appId, right }) => right === 'configure' ? owners[appId] === actor.accountId : members[appId]?.includes(actor.accountId) === true,
    readTrustedEvent(context) { if (!context || !proofs.has(context)) throw Object.assign(new Error('untrusted context'), { code: 'test_untrusted_context' }); return proofs.get(context); }, ...overrides };
  const reopen = () => { service?.close(); service = createReferralsService(options); return service; };
  reopen();
  t.after(() => { service.close(); for(const probe of probes)probe.close(); assert.equal(dirname(resolve(directory)), base); assert.ok(directory.startsWith(join(base, 'soty-referrals-test-'))); rmSync(directory, { recursive: true, force: true }); });
  const call = (op, args = {}, actor = owner) => service.execute({ op: `referrals.${op}`, args: { expectedAccountId: actor.accountId, appId: 'app-a', ...args }, actor });
  const apply = input => { const context = Object.freeze({}); proofs.set(context, structuredClone({ appId: 'app-a', ...input })); return service.internal.applyEvent({ context }); };
  const bind = (account, parent, appId = 'app-a') => apply({ kind: 'attribution', eventId: `attribution-${++serial}`, appId, accountId: account.accountId, parentAccountId: parent?.accountId ?? null });
  const chain = (appId = 'app-a') => { bind(payer, one, appId); bind(one, two, appId); bind(two, three, appId); bind(three, four, appId); bind(four, null, appId); };
  const pay = (paymentId, amount, extra = {}) => apply({ kind: 'payment', eventId: paymentId, payerAccountId: payer.accountId, currency: 'RUB', eligibleAmountMinor: amount, ...extra });
  const refund = (eventId, paymentId, refundAmountMinor, extra = {}) => apply({ kind: 'refund', eventId, paymentId, refundAmountMinor, ...extra });
  const balance = (actor = one, appId = 'app-a', moneyCurrency = 'RUB') => call('balance.get', { appId, currency: moneyCurrency }, actor).amountMinor;
  const openProbe=()=>{const probe=new DatabaseSync(databasePath);probes.push(probe);return probe;};
  return { get service() { return service; }, call, apply, bind, chain, pay, refund, balance, reopen, databasePath, proofs, options,openProbe };
}

test('defaults credit exactly three uplines at10% of eligible amount and no fourth beneficiary', t => {
  const f = fixture(t); assert.deepEqual(f.call('config.get').ratesBps, [1000, 1000, 1000]);
  f.chain(); const result = f.pay('payment-first', 10005);
  assert.deepEqual(result.credits.map(c => [c.beneficiaryAccountId, c.level, c.amountMinor]), [[one.accountId, 1, 1000], [two.accountId, 2, 1000], [three.accountId, 3, 1000]]);
  assert.equal(f.balance(four), '0'); assert.equal(f.balance(one), '1000'); assert.equal(result.payoutsEnabled, false);
  assert.deepEqual(f.call('ledger.list', {}, one).entries.map(e => e.amountMinor), [1000]);
});

test('two independent app scopes share no attribution, configuration, money or mutation namespace', t => {
  const f = fixture(t); f.chain('app-a'); f.bind(payer, two, 'app-b');
  f.call('config.put', { appId: 'app-b', mutationId: 'same-mutation', expectedRevision: 0, ratesBps: [5000] }, otherOwner);
  f.pay('same-payment', 1000); f.pay('same-payment', 1000, { appId: 'app-b' });
  assert.equal(f.balance(one, 'app-a'), '100'); assert.equal(f.balance(one, 'app-b'), '0'); assert.equal(f.balance(two, 'app-b'), '500');
  assert.deepEqual(f.call('config.get').ratesBps, [1000, 1000, 1000]);
  fail(() => f.call('config.get', { appId: 'app-b' }), 'referrals_forbidden');
  fail(() => f.call('config.put', { appId: 'app-a', mutationId: 'unauthorized', expectedRevision: 0, ratesBps: [1] }, otherOwner), 'referrals_forbidden');
  fail(() => f.refund('wrong-app-refund', 'only-in-a', 1, { appId: 'app-b' }), 'referrals_payment_not_found');
});

test('public read is own-account only; stale identity and client-selected beneficiary are rejected', t => {
  const f = fixture(t); f.chain(); f.pay('private-payment', 1000);
  assert.equal(f.call('ledger.list', {}, payer).entries.length, 0); assert.equal(f.balance(payer), '0');
  fail(() => f.call('balance.get', { currency: 'RUB', expectedAccountId: one.accountId }, payer), 'referrals_account_changed');
  fail(() => f.call('balance.get', { currency: 'RUB', beneficiaryAccountId: one.accountId }, payer), 'referrals_invalid_arguments');
  assert.equal(Object.hasOwn(f.call('ledger.list', {}, one).entries[0], 'paymentId'), false);
  fail(() => f.call('config.put', { mutationId: 'no-config-grant', expectedRevision: 0, ratesBps: [9000] }, payer), 'referrals_forbidden');
});

test('immutable attribution rejects self/cycles/change but accepts replay and same-parent reassertion', t => {
  const f = fixture(t); fail(() => f.bind(one, one), 'referrals_self_referral');
  const event = { kind: 'attribution', eventId: 'bind-one', accountId: one.accountId, parentAccountId: two.accountId };
  f.apply(event); assert.equal(f.apply(event).replayed, true); assert.equal(f.bind(one, two).alreadyBound, true);
  fail(() => f.bind(two, one), 'referrals_cycle'); fail(() => f.bind(one, three), 'referrals_attribution_immutable');
  fail(() => f.apply({ ...event, parentAccountId: three.accountId }), 'referrals_event_reused');
  f.bind(two, null); fail(() => f.bind(two, three), 'referrals_attribution_immutable');
});

test('config is owner-authorized CAS, mutation-idempotent and applied only to future payments', t => {
  const f = fixture(t); f.chain(); const first = f.pay('before-config', 1000);
  const args = { mutationId: 'config-edit', expectedRevision: 0, ratesBps: [2500, 500] };
  const config = f.call('config.put', args); assert.equal(config.revision, 1); assert.equal(f.call('config.put', args).replayed, true);
  fail(() => f.call('config.put', { ...args, ratesBps: [100] }), 'referrals_event_reused');
  fail(() => f.call('config.put', { ...args, mutationId: 'stale-config' }), 'referrals_revision_conflict');
  const second = f.pay('after-config', 1000); assert.deepEqual(second.credits.map(c => c.amountMinor), [250, 50]);
  assert.equal(first.configRevision, 0); assert.equal(second.configRevision, 1);
  const reversed = f.refund('refund-old-config', 'before-config', 1000); assert.deepEqual(reversed.reversals.map(r => r.amountMinor), [-100, -100, -100]);
  assert.equal(f.balance(one), '250'); assert.equal(f.balance(three), '0');
});

test('rates/levels are bounded and exact integer math beats floating-point control', t => {
  const f = fixture(t);
  for (const value of [[], [10001], [5001, 5000], [0.5], [-1], Array(11).fill(0), [NaN]]) assert.throws(() => f.call('config.put', { mutationId: 'invalid-config', expectedRevision: 0, ratesBps: value }));
  f.call('config.put', { mutationId: 'exact-config', expectedRevision: 0, ratesBps: [9999] }); f.bind(payer, one);
  const amount = 9007199254740990, expected = 9006298534815515;
  assert.notEqual(Math.floor(amount * 9999 / 10000), expected, 'negative control: binary multiplication loses one minor unit');
  assert.equal(f.pay('large-payment', amount).credits[0].amountMinor, expected);
  f.pay('large-payment-two', amount); assert.equal(f.balance(one), (BigInt(expected) * 2n).toString(), 'balance remains exact past JS safe integer total');
});

test('payment ID replay is durable; a changed eligible amount, payer or currency is not a new credit', t => {
  const f = fixture(t); f.chain(); const first = f.pay('stable-payment', 1000); const once = f.balance(one);
  assert.equal(f.pay('stable-payment', 1000).replayed, true); assert.equal(f.balance(one), once);
  f.reopen(); assert.deepEqual(f.pay('stable-payment', 1000), { ...first, replayed: true }); assert.equal(f.balance(one), once);
  for (const extra of [{ currency: 'USD' }, { payerAccountId: two.accountId }, { eligibleAmountMinor: 2000 }]) fail(() => f.pay('stable-payment', 1000, extra), 'referrals_event_reused');
  const duplicateBlindControl = Number(once) + first.credits[0].amountMinor;
  assert.throws(() => assert.equal(String(duplicateBlindControl), once), 'negative control: applying the same receipt twice changes balance');
});

test('partial refunds use cumulative original commissions and full reversal is exact across config changes', t => {
  const f = fixture(t); f.chain(); f.pay('rounded-payment', 19); assert.equal(f.balance(one), '1');
  const first = f.refund('refund-part-one', 'rounded-payment', 9); assert.equal(first.reversals[0].amountMinor, 0);
  assert.equal(f.refund('refund-part-one', 'rounded-payment', 9).replayed, true);
  f.call('config.put', { mutationId: 'rates-after-sale', expectedRevision: 0, ratesBps: [9900] });
  f.reopen(); const last = f.refund('refund-part-two', 'rounded-payment', 10);
  assert.deepEqual(last.reversals.map(r => r.amountMinor), [-1, -1, -1]); assert.equal(f.balance(one), '0');
  fail(() => f.refund('over-refund', 'rounded-payment', 1), 'referrals_refund_exceeds_payment');
  fail(() => f.refund('refund-part-one', 'rounded-payment', 8), 'referrals_event_reused');
  assert.equal(f.call('ledger.list', {}, one).entries.reduce((sum, row) => sum + row.amountMinor, 0), 0);
  const brokenReversed=Math.floor(1*9/19)+Math.floor(1*10/19);
  assert.throws(()=>assert.equal(brokenReversed,1),'negative control: independent rounding of each refund leaves an unreversed minor unit');
});

test('unattributed or zero-eligible payment is recorded without fictional beneficiaries', t => {
  const f = fixture(t); assert.deepEqual(f.pay('no-attribution', 1000).credits, []);
  f.bind(payer, one); assert.deepEqual(f.pay('zero-amount', 0).credits.map(c => c.amountMinor), [0]);
  fail(() => f.refund('zero-refund', 'zero-amount', 1), 'referrals_refund_exceeds_payment'); assert.equal(f.balance(one), '0');
});

test('trusted host port accepts only resolved proofs; no RPC or descriptor operation can fabricate credit', t => {
  const f = fixture(t); assert.deepEqual([...f.service.operations], REFERRAL_OPERATIONS);
  fail(() => f.call('payment', { eventId: 'fake', eligibleAmountMinor: 1000 }), 'unsupported_operation');
  fail(() => f.service.internal.applyEvent({ context: { kind: 'payment', appId: 'app-a', eventId: 'fake', eligibleAmountMinor: 1000 } }), 'test_untrusted_context');
  const noHost = createReferralsService({ databasePath: ':memory:', projectId: 'no-host', authorize: () => true });
  t.after(() => noHost.close()); fail(() => noHost.internal.applyEvent({ context: {} }), 'referrals_trusted_host_required');
  assert.equal(f.balance(one), '0');
});

test('COMMIT failure leaves no payment, ledger or receipt, then identical event is safely retryable', t => {
  const f = fixture(t); f.chain(); const probe = f.openProbe();
  probe.exec(`PRAGMA foreign_keys=ON; CREATE TABLE fault_parent(id INTEGER PRIMARY KEY); CREATE TABLE fault_child(id INTEGER REFERENCES fault_parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER fault_commit AFTER INSERT ON referral_ledger BEGIN INSERT INTO fault_child VALUES(987); END;`);
  assert.throws(() => f.pay('commit-failure', 1000), /FOREIGN KEY/);
  assert.equal(f.balance(one), '0');
  assert.equal(probe.prepare("SELECT COUNT(*) AS n FROM referral_payments WHERE payment_id='commit-failure'").get().n, 0);
  assert.equal(probe.prepare("SELECT COUNT(*) AS n FROM referral_receipts WHERE event_id='commit-failure'").get().n, 0);
  probe.exec('DROP TRIGGER fault_commit; DROP TABLE fault_child; DROP TABLE fault_parent;');
  assert.equal(f.pay('commit-failure', 1000).credits.length, 3); f.reopen(); assert.equal(f.balance(one), '100'); assert.equal(f.pay('commit-failure', 1000).replayed, true);
});

test('authorization revoked before COMMIT rolls config and receipt back', t => {
  let checks = 0, deny = true;
  const f = fixture(t, { authorize: ({ right }) => right === 'read' || !deny || ++checks < 3 });
  fail(() => f.call('config.put', { mutationId: 'revoked-config', expectedRevision: 0, ratesBps: [700] }), 'referrals_forbidden');
  assert.equal(f.call('config.get').revision, 0); deny = false;
  assert.equal(f.call('config.put', { mutationId: 'revoked-config', expectedRevision: 0, ratesBps: [700] }).revision, 1);
});

test('app proofs are rechecked before COMMIT and a changed proof cannot leave a partial credit', t => {
  let checks = 0, change = true;
  const f = fixture(t, { readTrustedEvent() { checks++; return { kind: 'payment', appId: 'app-a', eventId: 'changing-proof', payerAccountId: payer.accountId, currency: 'RUB', eligibleAmountMinor: change && checks === 3 ? 2000 : 1000 }; } });
  fail(() => f.service.internal.applyEvent({ context: {} }), 'referrals_trusted_event_changed'); change = false;
  assert.equal(f.service.internal.applyEvent({ context: {} }).eligibleAmountMinor, 1000);
});

test('ledger and attribution rows are immutable even through accidental SQL writes', t => {
  const f = fixture(t); f.chain(); f.pay('immutable-payment', 1000); const probe = f.openProbe();
  assert.throws(() => probe.exec('UPDATE referral_ledger SET amount=100000'), /referrals_record_immutable/);
  assert.throws(() => probe.exec('DELETE FROM referral_attributions'), /referrals_record_immutable/);
  assert.throws(() => probe.exec('DELETE FROM referral_receipts'), /referrals_record_immutable/);
  assert.equal(f.balance(one), '100');
});

test('bounded ledger paging is own-app and own-account and later appends remain readable', t => {
  const f = fixture(t); f.bind(payer, one); for (let i = 0; i < 4; i++) f.pay(`paged-payment-${i}`, 1000);
  const page = f.call('ledger.list', { limit: 2 }, one); assert.equal(page.entries.length, 2); assert.ok(page.nextCursor);
  const tail = f.call('ledger.list', { limit: 2, cursor: page.nextCursor }, one); assert.equal(tail.entries.length, 2); assert.equal(tail.nextCursor, undefined);
  assert.ok(tail.entries.every(e => e.entryId > page.nextCursor)); assert.equal(f.call('ledger.list', {}, two).entries.length, 0);
});

test('currency and numeric boundaries reject unsafe or fractional minor-unit events', t => {
  const f = fixture(t); for (const amount of [-1, 1.1, Infinity, Number.MAX_SAFE_INTEGER + 1, -0]) assert.throws(() => f.pay('invalid-payment', amount));
  assert.throws(() => f.pay('invalid-currency', 1, { currency: 'rub' }));
  assert.throws(() => f.pay('extra-fields', 1, { actor: owner }));
  f.bind(payer, one); f.pay('rub-payment', 1000); f.pay('usd-payment', 2000, { currency: 'USD' });
  assert.equal(f.balance(one), '100'); assert.equal(f.balance(one, 'app-a', 'USD'), '200');
});

test('persistent registry/project identity survives reopen and a different project is rejected', t => {
  const f = fixture(t), identity = f.service.registryId; f.chain(); f.call('config.put', { mutationId: 'persistent-config', expectedRevision: 0, ratesBps: [1000] }); f.pay('persistent-pay', 1000);
  f.reopen(); assert.equal(f.service.registryId, identity); assert.equal(f.call('config.get').revision, 1); assert.equal(f.balance(one), '100');
  fail(() => createReferralsService({ ...f.options, projectId: 'another-project' }), 'referrals_project_mismatch');
  assert.equal(f.call('attribution.get', {}, payer).parentAccountId, one.accountId);
});

test('asynchronous host callbacks and accessor-bearing inputs fail closed', t => {
  for (const option of ['authorize','readTrustedEvent','clock']) fail(()=>createReferralsService({databasePath:':memory:',projectId:'sync-test',authorize:()=>true,[option]:async()=>true}),'referrals_host_required');
  const f=fixture(t);let reads=0;
  const args={expectedAccountId:owner.accountId,appId:'app-a'};Object.defineProperty(args,'ratesBps',{enumerable:true,get(){reads++;return [1000];}});
  assert.throws(()=>f.service.execute({op:'referrals.config.put',actor:owner,args}));assert.equal(reads,0);
  assert.equal(f.call('config.get').revision,0);
});
