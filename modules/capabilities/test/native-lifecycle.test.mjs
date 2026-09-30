import test from 'node:test';
import assert from 'node:assert/strict';
import { connectedFixture, INPUT, code, identity, good, signed, ORIGIN } from './support/native-connected.mjs';

const counts = f => ({
  notes: f.sql('notes', db => db.prepare('SELECT count(*) AS n FROM notes').get().n),
  proofs: f.sql('notes', db => db.prepare('SELECT count(*) AS n FROM note_native_creates').get().n),
  ledger: f.sql('caps', db => db.prepare('SELECT count(*) AS n FROM cap_invocations').get().n),
  receipts: f.sql('caps', db => db.prepare('SELECT count(*) AS n FROM cap_receipts').get().n),
  budget: f.sql('caps', db => ({ ...db.prepare('SELECT reserved_amount,spent_amount FROM cap_budgets').get() })),
});

for (const seam of ['admission', 'marker', 'notes-commit', 'receipt-before-purge', 'caps-commit']) {
  test(`actual process kill at ${seam}: reopen keeps one identity and reconciles committed proof without reexecution`, async t => {
    const f = await connectedFixture(t), caller = await f.issue();
    const child = await f.child({ seam, token: caller.credential.token });
    const reached = child.wait('checkpoint'); child.start();
    assert.equal((await reached).seam, seam); await child.kill();
    const durable = counts(f), afterNote = ['notes-commit', 'receipt-before-purge', 'caps-commit'].includes(seam);
    assert.equal(durable.notes, Number(afterNote)); assert.equal(durable.proofs, Number(afterNote));
    assert.equal(durable.ledger, 1); assert.equal(durable.receipts, Number(seam === 'caps-commit'));
    assert.deepEqual(durable.budget, seam === 'caps-commit'
      ? { reserved_amount: 0, spent_amount: 1 } : { reserved_amount: 1, spent_amount: 0 });
    f.restart();
    const actor = f.actor(caller.credential.token), replay = f.native.admit({ actor, input: INPUT, idempotencyKey: 'native_crash_key' });
    assert.equal(replay.reused, true);
    const invocationId = replay.invocation.invocationId;
    if (afterNote) {
      // Revocation/cancel must never refund the already committed private note.
      if (seam === 'notes-commit') await f.access('grants.revoke', { grantId: caller.grant.id });
      if (seam === 'receipt-before-purge') f.caps.invocations.requestCancel({ actor, invocationId });
      assert.equal(f.native.reconcile({ invocationId }).outcome, 'committed');
      if (seam === 'notes-commit') assert.throws(() => f.native.get({ actor, invocationId }), code('access_denied'));
    } else {
      assert.equal(f.native.reconcile({ invocationId }).outcome, 'retryable');
      assert.equal(counts(f).notes, 0, 'reconcile never creates');
      f.native.beginAttempt({ invocationId });
      assert.equal(f.native.execute({ invocationId }).outcome, 'committed');
    }
    assert.deepEqual(counts(f), { notes: 1, proofs: 1, ledger: 1, receipts: 1, budget: { reserved_amount: 0, spent_amount: 1 } });
    assert.equal(f.sql('caps', db => db.prepare('SELECT input_json FROM cap_invocations').get().input_json), 'null');
    f.restart(); assert.equal(f.native.reconcile({ invocationId }).outcome, 'committed');
  });
}

test('two actual service processes serialize same-key admission and the final shared budget', async t => {
  for (const sameKey of [true, false]) {
    const f = await connectedFixture(t), caller = await f.issue({ budget: 1 });
    const first = await f.child({ token: caller.credential.token, key: 'race_key_first' });
    const second = await f.child({ token: caller.credential.token, key: sameKey ? 'race_key_first' : 'race_key_second' });
    const results = [first.wait('result'), second.wait('result')]; first.start(); second.start();
    const values = await Promise.all(results); await Promise.all([first.exit, second.exit]);
    assert.ok(values.every(value => value.ok || ['budget_exceeded','connect_authority_busy','native_storage_busy'].includes(value.code)));
    for (let i = 0; i < values.length; i++) {
      if (['connect_authority_busy','native_storage_busy'].includes(values[i].code)) {
        try { const done = f.create(caller.actor, INPUT, i === 0 || sameKey ? 'race_key_first' : 'race_key_second'); values[i] = { ok: true, invocationId: done.invocation.invocationId }; }
        catch (error) { assert.equal(error.code, 'budget_exceeded'); values[i] = { ok: false, code: error.code }; }
      }
    }
    assert.equal(values.filter(value => value.ok).length, sameKey ? 2 : 1);
    if (sameKey) assert.equal(values[0].invocationId, values[1].invocationId);
    assert.deepEqual(counts(f), { notes: 1, proofs: 1, ledger: 1, receipts: 1, budget: { reserved_amount: 0, spent_amount: 1 } });
  }
});

test('a real signed human writer and native writer cannot exceed the final Notes quota', async t => {
  const f = await connectedFixture(t, { notesLimits: { notes: 1 } }), caller = await f.issue();
  const request = await signed(f.connect, f.owner, 'notes.put', { expectedAccountId: f.account.accountId,
    noteId: 'human_parallel_note', mutationId: 'human_parallel_mutation', expectedRevision: 0,
    title: 'Human', body: 'Signed owner', items: [], pinned: false, color: 'plain', state: 'active' });
  const human = await f.child({ mode: 'signed', request, notesLimits: { notes: 1 } });
  const native = await f.child({ token: caller.credential.token, notesLimits: { notes: 1 } });
  const waits = [human.wait('result'), native.wait('result')]; human.start(); native.start();
  const results = await Promise.all(waits); await Promise.all([human.exit, native.exit]);
  assert.equal(results[1].ok, true); assert.ok(['committed','not_applied'].includes(results[1].outcome));
  assert.ok(results[0].ok || results[0].code === 'notes_count_quota');
  assert.equal(counts(f).notes, 1);
  assert.deepEqual(f.sql('notes', db => ({ ...db.prepare('SELECT active,identities FROM note_accounts').get() })), { active: 1, identities: 1 });
  assert.equal(counts(f).budget.reserved_amount, 0);
});

test('more than32 signed human revisions and purge retain only historical native creation, never current private text', async t => {
  const f = await connectedFixture(t), caller = await f.issue(), done = f.create(caller.actor), artifact = done.invocation.receipt.artifacts[0];
  for (let i = 0; i < 34; i++) await f.call('notes.put', { expectedAccountId: f.account.accountId,
    noteId: artifact.id, mutationId: `human_mutation_${i.toString().padStart(4, '0')}`, expectedRevision: i + 1,
    title: 'Changed', body: `private later ${i}`, items: [], pinned: false, color: 'plain', state: i === 33 ? 'trashed' : 'active' });
  await f.call('notes.purge', { expectedAccountId: f.account.accountId, noteId: artifact.id,
    mutationId: 'human_purge_final', expectedRevision: 35 });
  f.restart();
  const replay = f.native.admit({ actor: f.actor(caller.credential.token), input: INPUT, idempotencyKey: 'native_lifecycle_key' });
  assert.deepEqual(replay.invocation, done.invocation);
  assert.equal(f.native.execute({ invocationId: done.invocation.invocationId }).outcome, 'committed');
  assert.deepEqual(f.sql('notes', db => ({ ...db.prepare('SELECT state,title,body,revision FROM notes').get() })),
    { state: 'deleted', title: '', body: '', revision: 36 });
  assert.equal(f.sql('notes', db => db.prepare('SELECT count(*) AS n FROM note_native_creates').get().n), 1);
  assert.equal(f.sql('notes', db => db.prepare('SELECT count(*) AS n FROM notes_fts').get().n), 0);
  assert.ok(!JSON.stringify(replay).includes('private later'));
});

test('actual Connect creator revocation and original credential expiry close new effects without borrowing the retry actor', async t => {
  for (const reason of ['credential-expiry', 'creator-revoke']) {
    const f = await connectedFixture(t);
    const phone = identity('Approver phone');
    const start = await f.call('enrollment.start', { label: phone.label, encryptionPublicJwk: phone.encryptionPublicJwk }, phone);
    await f.call('enrollment.approve', { requestId: start.requestId, wrappedKey: { schema: 'test.encrypted', ciphertext: 'synthetic' } });
    await f.call('enrollment.finish', { requestId: start.requestId, expectedAccountId: f.account.accountId }, phone);
    const caller = await f.issue({ credentialExpiresAt: 2000 });
    const invocationId = f.native.admit({ actor: caller.actor, input: INPUT, idempotencyKey: 'authority_pair_key' }).invocation.invocationId;
    f.native.beginAttempt({ invocationId });
    const replacement = await f.access('credentials.issue', { grantId: caller.grant.id, audience: ORIGIN, expiresAt: 5000 });
    if (reason === 'creator-revoke') await f.call('device.revoke', { deviceId: f.account.deviceId }, phone);
    else f.time(2500);
    assert.equal(f.native.execute({ invocationId }).outcome, 'not_applied');
    if (reason === 'credential-expiry') assert.equal(f.native.get({ actor: f.actor(replacement.token), invocationId }).invocation.status, 'failed');
    else assert.throws(() => f.actor(replacement.token), code('access_denied'));
    assert.deepEqual(counts(f).budget, { reserved_amount: 0, spent_amount: 0 }); assert.equal(counts(f).notes, 0);
  }
});
