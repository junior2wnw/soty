import test from 'node:test';
import assert from 'node:assert/strict';
import { createNotesService } from '../../notes/server/index.mjs';
import { createCapabilitiesService } from '../server/index.mjs';
import { canonicalHash } from '../server/validation.mjs';
import { nativeFixture, PROJECT, OWNER, AUDIENCE, code } from './support/native-effect.mjs';

test('native composition exists in v1/mixed stores but cannot admit new effects; constructors reject async ports', t => {
  for (const versions of [{ notesMigration: false, capsMigration: false }, { notesMigration: false }, { capsMigration: false }]) {
    const f = nativeFixture(t, versions), identity = f.issue();
    assert.ok(f.native); assert.deepEqual(f.native.readiness(), { ready: false });
    assert.throws(() => f.native.admit({ actor: identity.actor, idempotencyKey: 'unavailable_key', input: { title: '', body: '' } }), code('native_unavailable'));
    assert.equal(f.sql(f.capsFile).prepare('SELECT count(*) AS n FROM cap_invocations').get().n, 0);
  }
  assert.throws(() => createCapabilitiesService({ databasePath: ':memory:', projectId: PROJECT, actorActive: () => true,
    nativeNotes: { notes: {}, withAuthorityFence: async () => {} } }), code('native_configuration_invalid'));
});

test('Notes native port is closed without verifier; preflight is pure and does not authorize a create', t => {
  const notes = createNotesService({ databasePath: ':memory:', projectId: PROJECT, allowNativeMigration: true });
  t.after(() => notes.close());
  const input = { title: 'Title', body: 'Body' };
  assert.equal(notes.native.validateDraftInput({ input }).documentBytes,
    Buffer.byteLength(JSON.stringify({ ...input, items: [], color: 'plain', pinned: false, state: 'active' })));
  assert.throws(() => notes.native.createDraftForInvocation({ context: {}, input }), code('native_unavailable'));
  assert.throws(() => notes.native.readCreateProof({ context: {} }), code('native_unavailable'));
});

test('real native admission, separate marker and effect produce one Notes proof/receipt/spend and purge input', t => {
  const f = nativeFixture(t), identity = f.issue(), input = { title: 'Private 😀', body: 'Exact е\u0301\ntext' };
  const capsDb = f.sql(f.capsFile), notesDb = f.sql(f.notesFile);
  assert.deepEqual(f.native.readiness(), { ready: true });
  const admitted = f.native.admit({ actor: identity.actor, idempotencyKey: 'native_exact_key', input });
  const invocationId = admitted.invocation.invocationId;
  assert.equal(notesDb.prepare('SELECT count(*) AS n FROM notes').get().n, 0);
  assert.throws(() => f.native.execute({ invocationId }), code('native_attempt_not_started'));
  assert.equal(f.native.beginAttempt({ invocationId }).started, true);
  assert.equal(notesDb.prepare('SELECT count(*) AS n FROM notes').get().n, 0);
  assert.equal(f.native.get({ actor: identity.actor, invocationId }).invocation.effectState, 'unknown');
  const done = f.native.execute({ invocationId });
  assert.equal(done.outcome, 'committed'); assert.equal(done.invocation.status, 'succeeded');
  const noteId = done.invocation.receipt.artifacts[0].id;
  assert.equal(noteId, `n_${canonicalHash(['soty.native-note.v1', f.caps.registryId, invocationId])}`);
  assert.equal(f.note('get', { noteId }).note.body, input.body);
  assert.equal(notesDb.prepare('SELECT count(*) AS n FROM note_native_creates').get().n, 1);
  assert.deepEqual({ ...capsDb.prepare('SELECT reserved_amount,spent_amount FROM cap_budgets').get() }, { reserved_amount: 0, spent_amount: 1 });
  assert.equal(capsDb.prepare('SELECT input_json FROM cap_invocations').get().input_json, 'null');
  assert.equal(f.native.execute({ invocationId }).outcome, 'committed');
  assert.equal(f.native.reconcile({ invocationId }).outcome, 'committed');
  assert.equal(f.native.beginAttempt({ invocationId }).started, false);
  assert.equal(notesDb.prepare('SELECT count(*) AS n FROM notes').get().n, 1);
  const projection = JSON.stringify(f.native.get({ actor: identity.actor, invocationId }));
  for (const forbidden of [input.title, input.body, identity.credential.token, f.notes.registryId, f.caps.registryId]) assert.ok(!projection.includes(forbidden));
});

test('Unicode and full Notes bytes refuse before admission; derived preview preserves a valid pair boundary', t => {
  const f = nativeFixture(t), identity = f.issue(), db = f.sql(f.capsFile);
  for (const input of [{ title: '\ud800', body: '' }, { title: '', body: '\udfff' }]) {
    assert.throws(() => f.native.admit({ actor: identity.actor, idempotencyKey: 'bad_unicode_key', input }), code('invalid_unicode'));
  }
  assert.throws(() => f.native.admit({ actor: identity.actor, idempotencyKey: 'too_large_full_note',
    input: { title: '', body: '中'.repeat(87371) + 'x'.repeat(9) } }), code('notes_note_too_large'));
  assert.equal(db.prepare('SELECT count(*) AS n FROM cap_invocations').get().n, 0);
  assert.equal(db.prepare('SELECT count(*) AS n FROM cap_budget_reservations').get().n, 0);
  const text = 'x'.repeat(179) + '😀 tail';
  const created = f.create(identity.actor, { title: '😀'.repeat(80), body: text });
  const note = f.note('get', { noteId: created.invocation.receipt.artifacts[0].id }).note;
  assert.equal(note.body, text); assert.equal(note.title.length, 160);
  assert.equal(note.preview.length, 179); assert.equal(note.preview.isWellFormed(), true);
});

test('declared-size input creates and reopens through the baseline reader despite larger request envelope', t => {
  const f = nativeFixture(t), identity = f.issue(), input = { title: '', body: '中'.repeat(87350) };
  const done = f.create(identity.actor, input);
  f.reopen();
  const actor = f.actor(identity.credential.token);
  assert.equal(f.native.get({ actor, invocationId: done.invocation.invocationId }).invocation.status, 'succeeded');
  assert.equal(f.note('get', { noteId: done.invocation.receipt.artifacts[0].id }).note.body, input.body);
});

test('lost response replays terminal before disable, missing Notes readiness and lowered mutable limits', t => {
  const f = nativeFixture(t), identity = f.issue(), input = { title: 'Own receipt', body: 'x'.repeat(300) };
  const done = f.create(identity.actor, input, 'lost_response_key');
  f.reopen({ enabled: false, invocationLimits: { inputBytes: 30 }, notesLimits: { noteBytes: 100 },
    port: { storageIdentity() { throw new Error('test_notes_is_unavailable'); } } });
  const actor = f.actor(identity.credential.token);
  assert.deepEqual(f.native.readiness(), { ready: false });
  const retry = f.native.admit({ actor, idempotencyKey: 'lost_response_key', input });
  assert.equal(retry.reused, true); assert.deepEqual(retry.invocation, done.invocation);
  assert.throws(() => f.native.admit({ actor, idempotencyKey: 'lost_response_key', input: { ...input, body: 'different' } }), code('invocation_request_conflict'));
  assert.equal(f.sql(f.capsFile).prepare('SELECT count(*) AS n FROM cap_budget_reservations').get().n, 1);
});

test('original credential expiry blocks the effect while a fresh same-grant credential can read the terminal fact', t => {
  const f = nativeFixture(t), identity = f.issue({ credentialExpiresAt: 2000 });
  const admitted = f.native.admit({ actor: identity.actor, idempotencyKey: 'old_credential_key', input: { title: '', body: 'Never created' } });
  const invocationId = admitted.invocation.invocationId;
  f.native.beginAttempt({ invocationId });
  const replacement = f.call('credentials.issue', { grantId: identity.grant.id, audience: AUDIENCE, expiresAt: 5000 });
  f.time(2500);
  const actor = f.actor(replacement.token), result = f.native.execute({ invocationId });
  assert.equal(result.outcome, 'not_applied'); assert.equal(result.invocation.status, 'failed');
  assert.equal(f.native.get({ actor, invocationId }).invocation.status, 'failed');
  assert.equal(f.sql(f.notesFile).prepare('SELECT count(*) AS n FROM notes').get().n, 0);
  f.time(1500);
  assert.equal(f.native.execute({ invocationId }).outcome, 'not_applied', 'terminal facts never reopen when the clock or authority becomes permissive');
});

test('sibling credential revocation does not invalidate an unaffected original chain', t => {
  const f = nativeFixture(t), identity = f.issue();
  const invocationId = f.native.admit({ actor: identity.actor, idempotencyKey: 'sibling_epoch_key', input: { title: '', body: 'Allowed' } }).invocation.invocationId;
  f.native.beginAttempt({ invocationId });
  const sibling = f.call('credentials.issue', { grantId: identity.grant.id, audience: AUDIENCE });
  f.call('credentials.revoke', { credentialId: sibling.credential.id });
  assert.equal(f.native.execute({ invocationId }).outcome, 'committed');
});

test('retained and copied native contexts never authorize a later Notes call', t => {
  const f = nativeFixture(t), identity = f.issue(), input = { title: '', body: 'One call' };
  f.create(identity.actor, input);
  assert.ok(f.captured.some(value => value.mode === 'create'));
  for (const { context, mode } of f.captured) {
    assert.throws(() => f.native.verifyContext(context, mode), code('native_context_invalid'));
    assert.throws(() => f.native.verifyContext({ ...context }, mode), code('native_context_invalid'));
  }
  const create = f.captured.find(value => value.mode === 'create').context;
  assert.throws(() => f.notes.native.createDraftForInvocation({ context: create, input }), code('native_context_invalid'));
  assert.equal(f.sql(f.notesFile).prepare('SELECT count(*) AS n FROM notes').get().n, 1);
});

test('native admission cannot substitute account, target, actor or accessor-backed input', t => {
  const f = nativeFixture(t), identity = f.issue(), input = { title: 'Original', body: 'Only typed own data' };
  const request = { actor: identity.actor, idempotencyKey: 'typed_boundary_key', input };
  assert.throws(() => f.native.admit({ ...request, accountId: OWNER.accountId }), code('invalid_input'));
  assert.throws(() => f.native.admit({ ...request, target: { kind: 'native', handler: 'notes.createDraft', version: 1 } }), code('invalid_input'));
  assert.throws(() => f.native.admit({ ...request, actor: { ...identity.actor } }), code('authorization_required'));
  let getterCalls = 0;
  const accessor = { body: input.body, get title() { getterCalls++; return input.title; } };
  assert.throws(() => f.native.admit({ ...request, input: accessor }), code('invalid_input'));
  const hiddenAccessor = Object.defineProperty({ body: input.body }, 'title', { get() { getterCalls++; return input.title; } });
  assert.throws(() => f.native.admit({ ...request, input: hiddenAccessor }), code('invalid_input'));
  assert.throws(() => f.notes.native.validateDraftInput({ input: hiddenAccessor }), code('notes_invalid_arguments'));
  assert.equal(getterCalls, 0);
  assert.equal(f.sql(f.capsFile).prepare('SELECT count(*) AS n FROM cap_invocations').get().n, 0);
  assert.equal(f.sql(f.notesFile).prepare('SELECT count(*) AS n FROM notes').get().n, 0);
});

test('malformed internal proof is an invariant failure and cannot settle an admitted effect as absent', t => {
  const f = nativeFixture(t, { port: { readCreateProof() { return {}; } } }), identity = f.issue();
  const invocationId = f.native.admit({ actor: identity.actor, idempotencyKey: 'bad_proof_key',
    input: { title: '', body: 'Retained until proven' } }).invocation.invocationId;
  f.native.beginAttempt({ invocationId });
  assert.throws(() => f.native.execute({ invocationId }), code('native_proof_invalid'));
  assert.equal(f.native.get({ actor: identity.actor, invocationId }).invocation.effectState, 'unknown');
  assert.equal(f.sql(f.capsFile).prepare('SELECT count(*) AS n FROM cap_receipts').get().n, 0);
  assert.deepEqual({ ...f.sql(f.capsFile).prepare('SELECT reserved_amount,spent_amount FROM cap_budgets').get() },
    { reserved_amount: 1, spent_amount: 0 });
  assert.equal(f.sql(f.notesFile).prepare('SELECT count(*) AS n FROM notes').get().n, 0);
});

test('real Notes quota refusal becomes a verified negative receipt rather than a lost reservation', t => {
  const f = nativeFixture(t, { notesLimits: { notes: 1 } }), identity = f.issue();
  f.create(identity.actor, { title: 'First', body: '' }, 'first_quota_key');
  const second = f.create(identity.actor, { title: 'Second', body: '' }, 'second_quota_key');
  assert.equal(second.outcome, 'not_applied'); assert.equal(second.invocation.receipt.errorCode, 'notes_count_quota');
  assert.equal(second.invocation.effectState, 'none');
  assert.deepEqual({ ...f.sql(f.capsFile).prepare('SELECT reserved_amount,spent_amount FROM cap_budgets').get() }, { reserved_amount: 0, spent_amount: 1 });
  f.reopen();
  assert.equal(f.native.reconcile({ invocationId: second.invocation.invocationId }).outcome, 'not_applied');
});
