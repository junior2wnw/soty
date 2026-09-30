import test from 'node:test';
import assert from 'node:assert/strict';
import { startNativeRecovery } from '../capabilities-recovery.js';
import { nativeHttpFixture, nativeIdentity, good } from './support/native-capability-http.mjs';

function timerFixture() {
  let active = null; const delays = [], handles = [];
  return { delays, handles, get active() { return active; },
    timers: {
      setTimeout(callback, delay) {
        assert.equal(active, null, 'at most one recovery turn is scheduled');
        delays.push(delay); const handle = { callback, unreferenced: false, unref() { this.unreferenced = true; } };
        active = handle; handles.push(handle); return handle;
      },
      clearTimeout(handle) { assert.equal(handle, active); active = null; },
    },
    fire() { assert.ok(active); const callback = active.callback; active = null; callback(); },
  };
}

test('recovery visits bounded pages, preserves a cursor after a failure, backs off and stops before late callbacks', () => {
  const f = timerFixture(), calls = [], first = Buffer.from('first').toString('base64url'); let step = 0;
  const recovery = startNativeRecovery({ timers: f.timers, coordinator: { reconcilePage(args) {
    calls.push(args); step++;
    if (step === 1) return { items: [{ invocationId: 'one', errorCode: 'native_reconciliation_failed' },
      { invocationId: 'two', outcome: 'committed' }], nextCursor: first };
    if (step < 5) throw new Error('private_exception_must_not_be_in_status');
    return { items: [{ invocationId: 'three', outcome: 'retryable' }], nextCursor: null };
  } } });
  assert.equal(f.delays[0], 1000); f.fire();
  assert.deepEqual({ ...recovery.status(), lastRunAt: null }, { lastRunAt: null, checked: 2, committed: 1, failed: 1, pending: 0, unavailable: false });
  assert.equal(f.delays.at(-1), 2000);
  f.fire(); f.fire(); f.fire();
  assert.deepEqual(f.delays.slice(-3), [4000, 8000, 16000]); assert.equal(recovery.status().unavailable, true);
  f.fire(); assert.equal(f.delays.at(-1), 30000);
  assert.deepEqual(calls, [{}, { cursor: first }, { cursor: first }, { cursor: first }, { cursor: first }]);
  const late = f.active.callback; recovery.close(); recovery.close(); late();
  assert.equal(calls.length, 5); assert.equal(f.active, null);
  assert.ok(f.handles.every(handle => handle.unreferenced));
  assert.equal(JSON.stringify(recovery.status()).includes('private_exception'), false);
});

test('malformed or asynchronous recovery responses back off without an unhandled rejection or zero-delay loop', async () => {
  for (const value of [() => Promise.reject(new Error('rejected trusted port')),
    () => ({ items: [], nextCursor: 'stalled' }), () => ({ items: Array(17).fill({ outcome: 'held' }), nextCursor: null }),
    () => ({ items: [{ errorCode: 'wrong' }], nextCursor: null })]) {
    const f = timerFixture(), recovery = startNativeRecovery({ timers: f.timers, coordinator: { reconcilePage: value } });
    for (let index = 0; index < 8; index++) f.fire();
    assert.equal(recovery.status().unavailable, true); assert.ok(f.delays.every(delay => delay >= 1000 && delay <= 60000));
    assert.equal(f.delays.at(-1), 60000); recovery.close();
    await new Promise(resolve => setImmediate(resolve));
  }
});

for (const capabilitiesVersion of [2, 3]) test(`actual Caps${capabilitiesVersion} host recovery settles an authorized cancellation while disabled, and never executes another pending intention`, async t => {
  const f = await nativeHttpFixture(t, { capabilitiesVersion }), owner = nativeIdentity('Автор'), account = await f.bootstrap(owner);
  const identity = await f.issue(owner, account.accountId), service = f.app.locals.capabilitiesService;
  const actor = service.authenticateCredential({ token: identity.token, audience: f.origin });
  const cancelled = service.nativeNotes.admit({ actor, idempotencyKey: 'recovery-cancel-01', input: { title: 'Отменить', body: 'Текст' } });
  const pending = service.nativeNotes.admit({ actor, idempotencyKey: 'recovery-pending-01', input: { title: 'Дождаться', body: 'Другой текст' } });
  service.invocations.requestCancel({ actor, invocationId: cancelled.invocation.invocationId });
  await f.restart({ enabled: false });
  assert.equal(f.app.locals.capabilitiesService.schemaVersion, capabilitiesVersion);
  assert.ok(f.app.locals.nativeRecovery, 'every admitted native schema keeps recovery when new execution is disabled');
  f.app.locals.nativeRecovery.close();
  const timers = timerFixture(), recovery = startNativeRecovery({ timers: timers.timers, coordinator: f.app.locals.capabilitiesService.nativeNotes });
  t.after(() => recovery.close()); timers.fire();
  assert.equal(recovery.status().checked, 2); assert.equal(recovery.status().pending, 1); assert.equal(recovery.status().unavailable, false);
  const rows = f.sql(f.capsFile, db => db.prepare('SELECT id,status,cancel_requested,input_json FROM cap_invocations ORDER BY id').all());
  assert.equal(rows.find(row => row.id === cancelled.invocation.invocationId).status, 'cancelled');
  const unresolved = rows.find(row => row.id === pending.invocation.invocationId);
  assert.equal(unresolved.cancel_requested, 0); assert.notEqual(unresolved.input_json, 'null');
  assert.equal(f.sql(f.notesFile, db => db.prepare('SELECT count(*) AS n FROM notes').get().n), 0);
  assert.deepEqual(f.sql(f.capsFile, db => ({ ...db.prepare('SELECT reserved_amount,spent_amount FROM cap_budgets').get() })),
    { reserved_amount: 1, spent_amount: 0 });
});
