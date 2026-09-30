import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { connectedFixture, INPUT, code, ORIGIN } from './support/native-connected.mjs';
import { nativeFixture } from './support/native-effect.mjs';

const invocation = f => f.sql('caps', db => ({ ...db.prepare('SELECT * FROM cap_invocations ORDER BY created_at,id LIMIT 1').get() }));
const noteCount = f => f.sql('notes', db => db.prepare('SELECT count(*) AS n FROM notes').get().n);
function admit(f, actor, key = 'native_fault_key') {
  const invocationId = f.native.admit({ actor, idempotencyKey: key, input: INPUT }).invocation.invocationId;
  f.native.beginAttempt({ invocationId }); return invocationId;
}

test('rejected Promises from normal-function native ports, verifier and fence are contained without unhandled child-process rejection', async t => {
  for (const rejectedPort of ['identity','validation','read','create','verifier','fence']) {
  const f = await connectedFixture(t), caller = await f.issue();
  const child = await f.child({ token: caller.credential.token, rejectedPort });
  const response = child.wait('result'); child.start();
  const expected = { identity: 'native_unavailable', validation: 'native_contract_mismatch' }[rejectedPort] || 'native_context_invalid';
  assert.deepEqual(await response, { phase: 'result', ok: false, code: expected });
  assert.deepEqual(await child.exit, { code: 0, signal: null }, 'controlled sync refusal must not leave an unhandled rejected Promise');
  assert.equal(noteCount(f), 0); assert.ok([null, undefined].includes(invocation(f).completed_at));
  }
});

test('a retained malformed-fence callback cannot create an admission after its caller has already returned', t => {
  let late;
  // Deliberately malformed trusted composition; this is not an external exploit
  // and does not claim that the real Connect fence behaves asynchronously.
  const f = nativeFixture(t, { fence(callback) { late = callback; } }), caller = f.issue();
  assert.throws(() => f.native.admit({ actor: caller.actor, idempotencyKey: 'late_callback_key', input: INPUT }), code('native_context_invalid'));
  assert.throws(() => late(), code('native_context_invalid'));
  assert.equal(f.sql(f.capsFile).prepare('SELECT count(*) AS n FROM cap_invocations').get().n, 0);
});

test('one unknown Notes incarnation cannot starve a later healthy proof in the same recovery page', async t => {
  const f = await connectedFixture(t), caller = await f.issue();
  const oldId = admit(f, caller.actor, 'old_notes_store_key');
  f.time(1001);
  f.restart({ notesName: 'notes-other', migrate: true, port: port => ({ ...port, createDraftForInvocation(args) {
    port.createDraftForInvocation(args); throw new Error('synthetic response lost after Notes commit');
  } }) });
  const newId = admit(f, f.actor(caller.credential.token), 'new_notes_store_key');
  assert.throws(() => f.native.execute({ invocationId: newId }), /synthetic response lost/u);
  f.restart({ port: port => port });
  const page = f.native.reconcilePage();
  assert.deepEqual(page, { items: [{ invocationId: oldId, errorCode: 'native_reconciliation_failed' },
    { invocationId: newId, outcome: 'committed' }], nextCursor: null });
  assert.equal(f.sql('caps', db => db.prepare('SELECT input_json FROM cap_invocations WHERE id=?').get(oldId).input_json), JSON.stringify({ body: INPUT.body, title: INPUT.title }));
  assert.deepEqual(f.sql('caps', db => ({ ...db.prepare('SELECT reserved_amount,spent_amount FROM cap_budgets').get() })),
    { reserved_amount: 1, spent_amount: 1 });
  assert.equal(f.sql('notes-other', db => db.prepare('SELECT count(*) AS n FROM note_native_creates').get().n), 1);
});

test('Notes/Caps COMMIT exceptions before or after the real commit never invent an absence or duplicate an effect', async t => {
  for (const store of ['notes', 'caps']) for (const after of [false, true]) {
    const f = await connectedFixture(t), caller = await f.issue(), invocationId = admit(f, caller.actor);
    const exec = DatabaseSync.prototype.exec, failure = new Error('synthetic commit outcome'); let tripped = false;
    try {
      DatabaseSync.prototype.exec = function(sql) {
        const belongs = sql === 'COMMIT' && this.prepare('SELECT 1 FROM sqlite_schema WHERE name=?').get(store === 'notes' ? 'note_native_creates' : 'cap_receipts');
        const effect = belongs && this.prepare(store === 'notes' ? 'SELECT count(*) AS n FROM note_native_creates' : 'SELECT count(*) AS n FROM cap_receipts').get().n > 0;
        if (!tripped && effect) { tripped = true; if (after) exec.call(this, sql); throw failure; }
        return exec.call(this, sql);
      };
      assert.throws(() => f.native.execute({ invocationId }), error => error === failure);
    } finally { DatabaseSync.prototype.exec = exec; }
    assert.equal(tripped, true);
    const committed = store === 'caps' || after;
    assert.equal(noteCount(f), Number(committed));
    assert.equal(f.native.reconcile({ invocationId }).outcome, committed ? 'committed' : 'retryable');
    if (!committed) assert.equal(f.native.execute({ invocationId }).outcome, 'committed');
    f.restart(); assert.equal(f.native.reconcile({ invocationId }).outcome, 'committed'); assert.equal(noteCount(f), 1);
  }
});

test('expiry during the last Notes verifier rolls the document back before verified negative completion', async t => {
  const f = await connectedFixture(t), caller = await f.issue({ credentialExpiresAt: 2000 });
  const invocationId = admit(f, caller.actor);
  let checks = 0;
  f.restart({ onVerify(_token, mode) { if (mode === 'create' && ++checks === 3) f.time(2000); } });
  assert.equal(f.native.execute({ invocationId }).outcome, 'not_applied'); assert.equal(checks, 3);
  assert.equal(noteCount(f), 0);
  assert.equal(f.sql('notes', db => db.prepare('SELECT count(*) AS n FROM note_native_creates').get().n), 0);
  assert.deepEqual(f.sql('caps', db => ({ ...db.prepare('SELECT reserved_amount,spent_amount FROM cap_budgets').get() })), { reserved_amount: 0, spent_amount: 0 });
});

test('wrong-mode, cloned and late-microtask tokens cannot reuse a live Notes reconciliation context', async t => {
  let f, captured, later, wrongMode;
  f = await connectedFixture(t, { port: port => ({ ...port, readCreateProof({ context }) {
    captured = context;
    assert.throws(() => f.native.verifyContext(context, 'create'), code('native_context_invalid'));
    assert.throws(() => f.native.verifyContext({ ...context }, 'reconcile'), code('native_context_invalid'));
    later = Promise.resolve().then(() => {
      assert.throws(() => f.native.verifyContext(context, 'reconcile'), code('native_context_invalid'));
      assert.throws(() => port.createDraftForInvocation({ context, input: INPUT }), code('native_context_invalid'));
    });
    wrongMode = true; return port.readCreateProof({ context });
  } }) });
  const caller = await f.issue(), invocationId = admit(f, caller.actor);
  assert.equal(f.native.reconcile({ invocationId }).outcome, 'retryable'); await later;
  assert.equal(wrongMode, true); assert.ok(captured); assert.equal(noteCount(f), 0);
});

test('changed store, live contract pin, closed Notes and malformed output retain marker/input/budget until real proof is readable', async t => {
  for (const fault of ['closed', 'store', 'pin', 'output']) {
    const f = await connectedFixture(t), caller = await f.issue(), invocationId = admit(f, caller.actor);
    if (fault === 'closed') f.notes.close();
    if (fault === 'store') f.restart({ port: port => ({ ...port, storageIdentity() { return { ...port.storageIdentity(), registryId: 'f'.repeat(32) }; } }) });
    if (fault === 'pin') f.sql('caps', db => db.prepare('UPDATE cap_contracts SET digest=?').run('0'.repeat(64)));
    if (fault === 'output') f.restart({ port: port => ({ ...port, createDraftForInvocation(args) { return { ...port.createDraftForInvocation(args), revision: 2 }; } }) });
    assert.throws(() => f.native.execute({ invocationId }), code({ closed: 'notes_service_closed', store: 'native_store_mismatch', pin: 'native_contract_mismatch', output: 'native_proof_invalid' }[fault]));
    assert.equal(invocation(f).completed_at, null); assert.notEqual(invocation(f).input_json, 'null');
    assert.deepEqual(f.sql('caps', db => ({ ...db.prepare('SELECT reserved_amount,spent_amount FROM cap_budgets').get() })), { reserved_amount: 1, spent_amount: 0 });
    if (fault === 'pin') f.sql('caps', db => db.prepare('UPDATE cap_contracts SET digest=?').run('95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204'));
    f.restart({ port: port => port });
    assert.equal(f.native.reconcile({ invocationId }).outcome, fault === 'output' ? 'committed' : 'retryable');
  }
});
