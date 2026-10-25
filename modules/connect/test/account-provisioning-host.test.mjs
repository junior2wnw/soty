import test from 'node:test';
import assert from 'node:assert/strict';
import { signedFixture } from './support/direct-signed-fixture.mjs';

test('private account enumeration is paged and account-created observers run only after a successful commit', async t => {
  const f = signedFixture(t), events = [];
  const stop = f.service.subscribeAccountCreated(event => {
    assert.equal(f.service.hasAccount(event.accountId), true, 'account must already be committed, outside any Connect action transaction');
    events.push(event.accountId); throw Error('observer failed after commit');
  });
  const a = await f.person('A'), b = await f.person('B'), c = await f.person('C');
  assert.deepEqual(events, [a.accountId, b.accountId, c.accountId]);
  const ordered = [a.accountId, b.accountId, c.accountId].sort();
  assert.deepEqual(f.service.listAccountIds({ limit: 2 }), ordered.slice(0, 2));
  assert.deepEqual(f.service.listAccountIds({ after: ordered[1], limit: 2 }), ordered.slice(2));
  const replay = await f.call(a, 'bootstrap', { label: 'A', encryptionPublicJwk: a.encryptionPublicJwk });
  assert.equal(replay.accountId, a.accountId); assert.equal(events.length, 3);
  const d = await f.enroll(a, 'Second device'); assert.equal(d.accountId, a.accountId); assert.equal(events.length, 3);
  assert.equal(f.service.hasAccount('acct_' + 'F'.repeat(32)), false);
  assert.throws(() => f.service.listAccountIds({ limit: 501 })); assert.throws(() => f.service.listAccountIds({ after: 'caller-sql' }));
  stop(); await f.person('D'); assert.equal(events.length, 3);
});

test('a larger extension response limit is explicit host configuration and does not widen other operations or arguments', async t => {
  const large = 'x'.repeat(2 * 1024 * 1024 + 32768);
  const bounded = { operations: new Set(['fixture.bounded']), execute: () => ({ data: large }) };
  const admitted = { operations: new Set(['fixture.admitted']), maxResultBytes: 8 * 1024 * 1024, executeAsync: async () => ({ data: large }) };
  const f = signedFixture(t, [bounded, admitted]), actor = await f.person('A');
  const denied = await f.raw(actor, 'fixture.bounded'); assert.equal(denied.error.code, 'extension_result_too_large');
  assert.equal((await f.call(actor, 'fixture.admitted')).data.length, large.length);
  admitted.maxResultBytes = Infinity;
  assert.equal((await f.call(actor, 'fixture.admitted')).data.length, large.length, 'validated declaration is captured once');
  const argumentsTooLarge = await f.raw(actor, 'fixture.admitted', { data: large }); assert.equal(argumentsTooLarge.error.code, 'arguments_too_large');
});
