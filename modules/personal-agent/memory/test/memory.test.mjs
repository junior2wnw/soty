import test from 'node:test';
import assert from 'node:assert/strict';
import { copyFileSync } from 'node:fs';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { openMemoryPartition, MemoryError } from '../index.mjs';
import { fixture, personal, community, record, error, deferred, tick } from './fixture.mjs';

test('lexical recall and export preserve provenance/freshness and exact idempotency', async t => {
  const f = fixture(t), request = { context: f.owner, mutationId: 'remember_a', record: record() };
  const created = await f.store.remember(request);
  assert.equal(created.revision, 1); assert.equal(created.replayed, false);
  assert.equal((await f.store.remember(request)).replayed, true);
  const result = await f.store.recall({ context: f.owner, query: 'lighthouse' });
  assert.equal(result.records.length, 1); assert.equal(result.records[0].source, 'synthetic:fixture');
  assert.equal(result.records[0].stale, false);
  const exported = f.store.export({ context: f.owner });
  assert.equal(exported.records.length, 1); assert.equal(exported.restoreFloor, 0);
  assert.equal(Object.hasOwn(exported.records[0], 'embedding'), false);
  await assert.rejects(f.store.remember({ ...request, record: record('record_a', 'changed') }), error('memory_mutation_reused'));
});

test('cross-account recall, supersede, delete and export fail before embedding or private projection', async t => {
  let embeds = 0;
  const f = fixture(t, { embedding: { id: 'synthetic:v1', dimensions: 2, embed: async () => { embeds++; return [1, 0]; } } });
  await f.store.remember({ context: f.owner, mutationId: 'seed', record: record() });
  const bob = f.context(personal('bob')), before = embeds;
  await assert.rejects(f.store.recall({ context: bob, query: 'lighthouse' }), error('memory_access_denied'));
  await assert.rejects(f.store.supersede({ context: bob, mutationId: 'attack', records: [{ id: 'record_a', expectedRevision: 1 }], record: record('new_a') }), error('memory_access_denied'));
  assert.throws(() => f.store.delete({ context: bob, mutationId: 'attack', record: { id: 'record_a', expectedRevision: 1 } }), error('memory_access_denied'));
  assert.throws(() => f.store.export({ context: bob }), error('memory_access_denied'));
  assert.equal(embeds, before); assert.equal(f.advances.length, 0);
  assert.equal((await f.store.recall({ context: f.owner, query: 'lighthouse' })).records[0].text, record().text);
});

test('personal, project and group partitions cannot retrieve, count or supersede each other', async t => {
  const f = fixture(t);
  await f.store.remember({ context: f.owner, mutationId: 'seed', record: record() });
  for (const scope of [community(), personal('alice', 'project_one'), { ...personal(), audienceKind: 'project', audienceId: 'project_two', projectId: 'project_two' }]) {
    const context = f.context(scope), other = f.open({ scope, context, databasePath: join(f.directory, `${scope.audienceKind}_${scope.projectId ?? 'group'}.sqlite`) });
    assert.deepEqual(await other.recall({ context, query: 'lighthouse' }), { records: [] });
    assert.equal(other.export({ context }).records.length, 0);
    await assert.rejects(other.supersede({ context, mutationId: 'cross_scope', records: [{ id: 'record_a', expectedRevision: 1 }], record: record('new_a') }), error('memory_revision_conflict'));
    assert.throws(() => other.delete({ context, mutationId: 'cross_scope', record: { id: 'record_a', expectedRevision: 1 } }), error('memory_revision_conflict'));
    await assert.rejects(f.store.recall({ context, query: 'lighthouse' }), error('memory_access_denied'));
  }
});

test('same logical record ID is independent across physical account partitions', async t => {
  const f = fixture(t), bobScope = personal('bob'), bob = f.context(bobScope);
  const second = f.open({ scope: bobScope, context: bob, databasePath: join(f.directory, 'bob.sqlite') });
  await f.store.remember({ context: f.owner, mutationId: 'same_operation', record: record() });
  await second.remember({ context: bob, mutationId: 'same_operation', record: record('record_a', 'synthetic lighthouse Bob') });
  assert.equal((await second.recall({ context: bob, query: 'lighthouse' })).records[0].text, 'synthetic lighthouse Bob');
  assert.equal((await f.store.recall({ context: f.owner, query: 'lighthouse' })).records[0].text, record().text);
});

test('model payload cannot select tenant, path, audience or an unrecognised ACL field', async t => {
  const f = fixture(t);
  for (const key of ['tenantId', 'accountId', 'scope', 'databasePath', 'grants']) {
    await assert.rejects(f.store.remember({ context: f.owner, mutationId: `attack_${key}`, record: record(), [key]: 'chosen-by-model' }), error('memory_invalid_arguments'));
    await assert.rejects(f.store.recall({ context: f.owner, query: 'secret', [key]: 'chosen-by-model' }), error('memory_invalid_arguments'));
  }
  assert.throws(() => f.store.export({ context: Object.freeze({}) }), error('memory_access_denied'));
});

test('binding is immutable and opening another audience/account on the same SQLite rejects', async t => {
  const scope = personal(), f = fixture(t, { disk: true, scope });
  scope.accountId = 'bob';
  await f.store.remember({ context: f.owner, mutationId: 'seed', record: record() });
  const bobScope = personal('bob'), bob = f.context(bobScope);
  assert.throws(() => f.open({ scope: bobScope, context: bob }), error('memory_partition_mismatch'));
  assert.equal(f.store.export({ context: f.owner }).scope.accountId, 'alice');
});

test('synchronous admission is mandatory; Promise/async providers cannot grant access', t => {
  const f = fixture(t), file = join(f.directory, 'never_opened.sqlite');
  assert.throws(() => openMemoryPartition({ ...f.options, databasePath: file, verifyContext: async () => true }), error('memory_trusted_provider_required'));
  assert.throws(() => openMemoryPartition({ ...f.options, databasePath: file, verifyContext: () => Promise.resolve(true) }), error('memory_access_denied'));
  assert.throws(() => openMemoryPartition({ ...f.options, databasePath: file, readRestoreFloor: () => Promise.resolve(0) }), error('memory_restore_authority_unavailable'));
  assert.throws(() => openMemoryPartition({ ...f.options, databasePath: file, advanceRestoreFloor: undefined }), error('memory_trusted_provider_required'));
});

test('supersede removes previous text/vector/index and leaves no-content tombstones', async t => {
  const f = fixture(t, { disk: true, embedding: { id: 'synthetic:v1', dimensions: 2, embed: async () => [1, 0] } });
  await f.store.remember({ context: f.owner, mutationId: 'seed', record: record() });
  const args = { context: f.owner, mutationId: 'replace_a', records: [{ id: 'record_a', expectedRevision: 1 }], record: record('record_b', 'updated beacon') };
  const result = await f.store.supersede(args);
  assert.equal(result.restoreFloor, 1); assert.deepEqual(result.replaced, [{ id: 'record_a', revision: 2 }]);
  assert.equal((await f.store.supersede(args)).replayed, true);
  const exported = f.store.export({ context: f.owner });
  assert.deepEqual(exported.records.map(row => row.id), ['record_b']);
  assert.equal(exported.tombstones.length, 1); assert.equal(JSON.stringify(exported.tombstones).includes('lighthouse'), false);
  const raw = new DatabaseSync(f.databasePath, { readOnly: true });
  try {
    assert.equal(raw.prepare('SELECT count(*) AS count FROM memory_records WHERE id=?').get('record_a').count, 0);
    assert.equal(raw.prepare('SELECT count(*) AS count FROM memory_search WHERE id=?').get('record_a').count, 0);
  } finally { raw.close(); }
  await assert.rejects(f.store.remember({ context: f.owner, mutationId: 'resurrection', record: record() }), error('memory_record_exists'));
});

test('version conflicts fail without erasure/floor change or partial replacement', async t => {
  const f = fixture(t);
  await f.store.remember({ context: f.owner, mutationId: 'seed', record: record() });
  assert.throws(() => f.store.delete({ context: f.owner, mutationId: 'delete_wrong', record: { id: 'record_a', expectedRevision: 2 } }), error('memory_revision_conflict'));
  await assert.rejects(f.store.supersede({ context: f.owner, mutationId: 'replace_wrong', records: [{ id: 'record_a', expectedRevision: 1 }, { id: 'missing', expectedRevision: 1 }], record: record('new_a') }), error('memory_revision_conflict'));
  assert.equal(f.advances.length, 0); assert.equal(f.store.export({ context: f.owner }).records.length, 1);
});

test('delete is replayable; restored pre-delete snapshot is rejected by independent floor', async t => {
  const f = fixture(t, { disk: true });
  const add = { context: f.owner, mutationId: 'seed', record: record() };
  await f.store.remember(add); f.store.close();
  const beforeDelete = join(f.directory, 'before-delete.sqlite'); copyFileSync(f.databasePath, beforeDelete);
  const current = f.open(), args = { context: f.owner, mutationId: 'delete_a', record: { id: 'record_a', expectedRevision: 1 } };
  assert.equal(current.delete(args).restoreFloor, 1); assert.equal(current.delete(args).replayed, true);
  assert.deepEqual(await current.recall({ context: f.owner, query: 'lighthouse' }), { records: [] });
  assert.equal((await current.remember(add)).replayed, true);
  assert.equal(current.export({ context: f.owner }).records.length, 0);
  assert.throws(() => f.open({ databasePath: beforeDelete }), error('memory_restore_floor_mismatch'));
  current.close(); const reopened = f.open();
  assert.equal(reopened.export({ context: f.owner }).tombstones[0].id, 'record_a');
});

test('external floor advanced then local failure produces fail-closed unknown, never old data', async t => {
  const f = fixture(t, { disk: true });
  await f.store.remember({ context: f.owner, mutationId: 'seed', record: record() });
  f.onAdvance(() => { throw new Error('synthetic external ACK lost'); });
  assert.throws(() => f.store.delete({ context: f.owner, mutationId: 'unknown_delete', record: { id: 'record_a', expectedRevision: 1 } }), error('memory_restore_authority_unavailable'));
  assert.throws(() => f.store.export({ context: f.owner }), error('memory_restore_floor_mismatch'));
  await assert.rejects(f.store.recall({ context: f.owner, query: 'lighthouse' }), error('memory_restore_floor_mismatch'));
  f.store.close(); assert.throws(() => f.open(), error('memory_restore_floor_mismatch'));
});

test('no lease renewal, epoch switch or revocation can commit a late embedding', async t => {
  for (const change of [state => { state.epoch++; }, state => { state.leaseId = 'lease_new'; }, state => { state.active = false; }, state => { state.expiresAt = 1000; }]) {
    await t.test(change.toString(), async sub => {
      const gate = deferred(), f = fixture(sub, { embedding: { id: 'synthetic:v1', dimensions: 2, embed: () => gate.promise } });
      const work = f.store.remember({ context: f.owner, mutationId: 'late_write', record: record() });
      await tick(); change(f.state(f.owner)); gate.resolve([1, 0]);
      await assert.rejects(work, error(f.state(f.owner).active && f.state(f.owner).expiresAt > 1000 ? 'memory_context_stale' : 'memory_access_denied'));
      f.state(f.owner).active = true; f.state(f.owner).expiresAt = 200000;
      assert.equal(f.store.export({ context: f.owner }).records.length, 0);
    });
  }
});

test('late semantic result cannot disclose a private snippet after context revocation', async t => {
  const gate = deferred(); let queryPending = false;
  const f = fixture(t, { embedding: { id: 'synthetic:v1', dimensions: 2, embed: (_text, { purpose }) => purpose === 'query' && queryPending ? gate.promise : [1, 0] } });
  await f.store.remember({ context: f.owner, mutationId: 'seed', record: record() });
  queryPending = true;
  const work = f.store.recall({ context: f.owner, query: 'lighthouse' });
  await tick(); f.state(f.owner).active = false; gate.resolve([1, 0]);
  await assert.rejects(work, error('memory_access_denied'));
});

test('floor change cancels in-flight remember even if the live lease is unchanged', async t => {
  const gate = deferred(); let delay = false;
  const f = fixture(t, { embedding: { id: 'synthetic:v1', dimensions: 2, embed: () => delay ? gate.promise : [1, 0] } });
  await f.store.remember({ context: f.owner, mutationId: 'seed', record: record() });
  delay = true; const work = f.store.remember({ context: f.owner, mutationId: 'late_after_delete', record: record('record_b') });
  await tick(); f.store.delete({ context: f.owner, mutationId: 'delete_a', record: { id: 'record_a', expectedRevision: 1 } });
  gate.resolve([1, 0]); await assert.rejects(work, error('memory_context_stale'));
  assert.equal(f.store.export({ context: f.owner }).records.length, 0);
});

test('cancel retains an ignored encoder slot until real settlement and prevents late writes', async t => {
  const gate = deferred(); let first = true;
  const f = fixture(t, { limits: { embeddingConcurrency: 1 }, embedding: { id: 'synthetic:v1', dimensions: 2, embed: () => { if (first) { first = false; return gate.promise; } return [1, 0]; } } });
  const cancel = new AbortController();
  const work = f.store.remember({ context: f.owner, signal: cancel.signal, mutationId: 'cancel_a', record: record() });
  await tick(); cancel.abort(); await assert.rejects(work, error('memory_cancelled'));
  await assert.rejects(f.store.remember({ context: f.owner, mutationId: 'blocked', record: record('record_b') }), error('memory_busy'));
  gate.resolve([1, 0]); await tick();
  await f.store.remember({ context: f.owner, mutationId: 'later', record: record('record_b') });
  assert.deepEqual(f.store.export({ context: f.owner }).records.map(row => row.id), ['record_b']);
});

test('cancel before scheduled embedding and close do not start or commit late work', async t => {
  let calls = 0; const gate = deferred();
  const f = fixture(t, { embedding: { id: 'synthetic:v1', dimensions: 2, embed: () => { calls++; return gate.promise; } } });
  const cancel = new AbortController();
  const work = f.store.remember({ context: f.owner, signal: cancel.signal, mutationId: 'cancel_a', record: record() });
  cancel.abort(); await assert.rejects(work, error('memory_cancelled')); assert.equal(calls, 0);
  const second = f.store.remember({ context: f.owner, mutationId: 'close_a', record: record() });
  await tick(); f.store.close(); await assert.rejects(second, error('memory_closed'));
  gate.resolve([1, 0]); await tick();
});

test('late rejection from an ignored encoder is handled and releases the real slot', async t => {
  const gate = deferred(); let calls = 0;
  const f = fixture(t, { limits: { embeddingConcurrency: 1 }, embedding: { id: 'synthetic:v1', dimensions: 2, embed: () => ++calls === 1 ? gate.promise : [1, 0] } });
  const cancel = new AbortController(), work = f.store.remember({ context: f.owner, signal: cancel.signal, mutationId: 'cancel_a', record: record() });
  await tick(); cancel.abort(); await assert.rejects(work, error('memory_cancelled'));
  gate.reject(new Error('synthetic ignored late error')); await tick();
  await f.store.remember({ context: f.owner, mutationId: 'retry_other', record: record('record_b') });
  assert.equal(f.store.export({ context: f.owner }).records.length, 1);
});

test('expiry is enforced for query/export and again before async record commit', async t => {
  const f = fixture(t), request = { context: f.owner, mutationId: 'seed', record: record() };
  await f.store.remember(request); f.setClock(60000);
  assert.equal((await f.store.recall({ context: f.owner, query: 'lighthouse' })).records[0].stale, true);
  f.setClock(100000);
  assert.deepEqual(await f.store.recall({ context: f.owner, query: 'lighthouse' }), { records: [] });
  assert.equal(f.store.export({ context: f.owner }).records.length, 0);
  assert.equal((await f.store.remember(request)).replayed, true);
  await assert.rejects(f.store.remember({ context: f.owner, mutationId: 'expired_new', record: record('record_b') }), error('memory_retention_invalid'));
  const gate = deferred(), other = fixture(t, { embedding: { id: 'synthetic:v1', dimensions: 2, embed: () => gate.promise } });
  const late = other.store.remember({ context: other.owner, mutationId: 'late_expired', record: record() });
  await tick(); other.setClock(100000); gate.resolve([1, 0]); await assert.rejects(late, error('memory_retention_invalid'));
  assert.equal(other.store.export({ context: other.owner }).records.length, 0);
});

test('final admission is checked before delivering an export', async t => {
  const f = fixture(t); await f.store.remember({ context: f.owner, mutationId: 'seed', record: record() });
  let checks = 0;
  f.onVerify(request => { if (request.operation === 'export' && ++checks === 2) f.state(f.owner).active = false; });
  assert.throws(() => f.store.export({ context: f.owner }), error('memory_access_denied'));
});

test('capacity and vector validation fail before partial erasure or floor advance', async t => {
  const f = fixture(t, { limits: { records: 1 } });
  await f.store.remember({ context: f.owner, mutationId: 'seed', record: record() });
  await assert.rejects(f.store.remember({ context: f.owner, mutationId: 'too_many', record: record('record_b') }), error('memory_capacity'));
  await f.store.supersede({ context: f.owner, mutationId: 'replace_capacity', records: [{ id: 'record_a', expectedRevision: 1 }], record: record('record_b') });
  assert.equal(f.store.export({ context: f.owner }).records.length, 1);
  const bad = fixture(t, { embedding: { id: 'bad:v1', dimensions: 2, embed: async () => [1, NaN] } });
  await assert.rejects(bad.store.remember({ context: bad.owner, mutationId: 'invalid_vector', record: record() }), error('memory_embedding_invalid'));
  assert.equal(bad.store.export({ context: bad.owner }).records.length, 0);
});

test('schema drift and encoder drift fail closed without implicit migration', async t => {
  const f = fixture(t, { disk: true }); f.store.close();
  assert.throws(() => f.open({ embedding: { id: 'changed:v1', dimensions: 2, embed: () => [1, 0] } }), error('memory_embedding_mismatch'));
  const raw = new DatabaseSync(f.databasePath); try { raw.exec('CREATE TABLE injected(id TEXT)'); } finally { raw.close(); }
  assert.throws(() => f.open(), error('memory_schema_unsupported'));
});

test('two live SQLite handles cannot both supersede the same expected revision', async t => {
  let delayed = false; const firstGate = deferred(), secondGate = deferred(); let calls = 0;
  const f = fixture(t, { disk: true, embedding: { id: 'synthetic:v1', dimensions: 2, embed: () => delayed ? (++calls === 1 ? firstGate.promise : secondGate.promise) : [1, 0] } });
  await f.store.remember({ context: f.owner, mutationId: 'seed', record: record() });
  const other = f.open(); delayed = true;
  const first = f.store.supersede({ context: f.owner, mutationId: 'first_writer', records: [{ id: 'record_a', expectedRevision: 1 }], record: record('record_b') });
  const second = other.supersede({ context: f.owner, mutationId: 'second_writer', records: [{ id: 'record_a', expectedRevision: 1 }], record: record('record_c') });
  await tick(); firstGate.resolve([1, 0]); await first;
  secondGate.resolve([1, 0]); await assert.rejects(second, error('memory_context_stale'));
  assert.deepEqual(other.export({ context: f.owner }).records.map(row => row.id), ['record_b']);
  assert.equal(f.advances.length, 1);
});

test('input snapshot, Unicode and FTS syntax cannot turn record data into authority or SQL', async t => {
  const gate = deferred(), f = fixture(t, { embedding: { id: 'synthetic:v1', dimensions: 2, embed: (_value, { purpose }) => purpose === 'record' ? gate.promise : [1, 0] } });
  const payload = record(), args = { context: f.owner, mutationId: 'seed', record: payload };
  const work = f.store.remember(args); payload.text = 'changed after validation'; args.context = Object.freeze({}); gate.resolve([1, 0]); await work;
  assert.equal(f.store.export({ context: f.owner }).records[0].text, record().text);
  assert.equal((await f.store.recall({ context: f.owner, query: 'lighthouse OR "bad" NEAR(attack)' })).records.length, 1);
  await assert.rejects(f.store.remember({ context: f.owner, mutationId: 'invalid_unicode', record: record('record_bad', '\ud800') }), error('memory_invalid_arguments'));
});

test('miswired synchronous providers returning rejected Promises fail closed without late host rejection', async t => {
  const f = fixture(t);
  assert.throws(() => openMemoryPartition({ ...f.options, databasePath: ':memory:', verifyContext: () => Promise.reject(new Error('synthetic private callback error')) }), error('memory_access_denied'));
  assert.throws(() => openMemoryPartition({ ...f.options, databasePath: ':memory:', readRestoreFloor: () => Promise.reject(new Error('synthetic private callback error')) }), error('memory_restore_authority_unavailable'));
  const other = f.open({ databasePath: ':memory:', advanceRestoreFloor: () => Promise.reject(new Error('synthetic private callback error')) });
  await other.remember({ context: f.owner, mutationId: 'seed', record: record() });
  assert.throws(() => other.delete({ context: f.owner, mutationId: 'bad_async_delete', record: { id: 'record_a', expectedRevision: 1 } }), error('memory_restore_authority_unavailable'));
  await tick(); assert.equal(other.export({ context: f.owner }).records.length, 1);
});

test('encoder deadline is bounded, keeps its ignored slot and cannot commit after timeout', async t => {
  const gate = deferred();
  const f = fixture(t, { limits: { embeddingConcurrency: 1, embeddingTimeoutMs: 5 }, embedding: { id: 'synthetic:v1', dimensions: 2, embed: () => gate.promise } });
  await assert.rejects(f.store.remember({ context: f.owner, mutationId: 'timeout', record: record() }), error('memory_embedding_timeout'));
  await assert.rejects(f.store.remember({ context: f.owner, mutationId: 'while_running', record: record('record_b') }), error('memory_busy'));
  gate.resolve([1, 0]); await tick();
  assert.equal(f.store.export({ context: f.owner }).records.length, 0);
});

test('live revocation before COMMIT rolls back local effect and refuses the advanced floor', async t => {
  const f = fixture(t);
  await f.store.remember({ context: f.owner, mutationId: 'seed', record: record() });
  f.onAdvance(() => { f.state(f.owner).active = false; });
  assert.throws(() => f.store.delete({ context: f.owner, mutationId: 'revoke_during_erase', record: { id: 'record_a', expectedRevision: 1 } }), error('memory_access_denied'));
  f.state(f.owner).active = true;
  assert.throws(() => f.store.export({ context: f.owner }), error('memory_restore_floor_mismatch'));
});

test('final recall guard can deny delivery after SQL without disclosing count or snippet', async t => {
  const f = fixture(t); await f.store.remember({ context: f.owner, mutationId: 'seed', record: record() });
  let reads = 0;
  f.onVerify(request => { if (request.operation === 'recall' && ++reads === 3) f.state(f.owner).active = false; });
  await assert.rejects(f.store.recall({ context: f.owner, query: 'lighthouse' }), error('memory_access_denied'));
});

test('encoder error class, code, message, cause and private details cannot leak to the caller', async t => {
  for (const code of ['synthetic_private_marker', 'memory_access_denied']) {
    const f = fixture(t, { embedding: { id: 'synthetic:errors', dimensions: 2, embed: () => {
      const failure = new MemoryError(code); failure.message = 'synthetic_private_marker';
      failure.details = { privateText: 'synthetic_private_marker' }; failure.cause = new Error('synthetic_private_marker'); throw failure;
    } } });
    await assert.rejects(f.store.remember({ context: f.owner, mutationId: 'bad_encoder', record: record() }), failure => {
      assert.equal(failure.code, 'memory_embedding_failed'); assert.equal(failure.message, 'memory_embedding_failed');
      assert.equal(Object.hasOwn(failure, 'cause'), false); assert.equal(Object.hasOwn(failure, 'details'), false);
      assert.equal(JSON.stringify(failure).includes('synthetic_private_marker'), false); return true;
    });
  }
});

test('metadata admission headroom leaves every extant record deletable after bounded churn', async t => {
  const f = fixture(t, { limits: { receipts: 10, tombstones: 3 } });
  await f.store.remember({ context: f.owner, mutationId: 'remember_a', record: record('record_a') });
  await f.store.remember({ context: f.owner, mutationId: 'remember_b', record: record('record_b') });
  await f.store.remember({ context: f.owner, mutationId: 'remember_c', record: record('record_c') });
  await assert.rejects(f.store.remember({ context: f.owner, mutationId: 'remember_d', record: record('record_d') }), error('memory_tombstone_capacity'));
  for (const suffix of ['a', 'b', 'c']) f.store.delete({ context: f.owner, mutationId: `delete_${suffix}`, record: { id: `record_${suffix}`, expectedRevision: 1 } });
  assert.equal(f.store.export({ context: f.owner }).records.length, 0);
  assert.equal(f.store.export({ context: f.owner }).tombstones.length, 3);
  const receipt = fixture(t, { limits: { receipts: 2 } });
  await receipt.store.remember({ context: receipt.owner, mutationId: 'remember_a', record: record() });
  await assert.rejects(receipt.store.remember({ context: receipt.owner, mutationId: 'remember_b', record: record('record_b') }), error('memory_receipt_capacity'));
  assert.equal(receipt.store.delete({ context: receipt.owner, mutationId: 'delete_a', record: { id: 'record_a', expectedRevision: 1 } }).erased, true);
  const churn = fixture(t, { limits: { receipts: 3, tombstones: 10 } });
  await churn.store.remember({ context: churn.owner, mutationId: 'seed', record: record() });
  await churn.store.supersede({ context: churn.owner, mutationId: 'replace_b', records: [{ id: 'record_a', expectedRevision: 1 }], record: record('record_b') });
  await assert.rejects(churn.store.supersede({ context: churn.owner, mutationId: 'replace_c', records: [{ id: 'record_b', expectedRevision: 1 }], record: record('record_c') }), error('memory_receipt_capacity'));
  assert.equal(churn.store.delete({ context: churn.owner, mutationId: 'delete_final', record: { id: 'record_b', expectedRevision: 1 } }).erased, true);
  assert.equal(churn.store.export({ context: churn.owner }).records.length, 0);
});

test('tiny caps refuse new admission before content exists; lowering caps cannot block existing erasure', async t => {
  const tiny = fixture(t, { limits: { receipts: 1 } });
  await assert.rejects(tiny.store.remember({ context: tiny.owner, mutationId: 'blocked_admission', record: record() }), error('memory_receipt_capacity'));
  assert.equal(tiny.store.export({ context: tiny.owner }).records.length, 0);
  const f = fixture(t, { disk: true });
  await f.store.remember({ context: f.owner, mutationId: 'remember_a', record: record('record_a') });
  await f.store.remember({ context: f.owner, mutationId: 'remember_b', record: record('record_b') });
  f.store.close(); const reduced = f.open({ limits: { records: 1, contentBytes: 1, receipts: 1, tombstones: 1 } });
  for (const suffix of ['a', 'b']) assert.equal(reduced.delete({ context: f.owner, mutationId: `delete_${suffix}`, record: { id: `record_${suffix}`, expectedRevision: 1 } }).erased, true);
  assert.equal(reduced.export({ context: f.owner }).records.length, 0);
});

test('fresh empty database at nonzero external floor cannot forget erased IDs or replay old remember', async t => {
  const f = fixture(t), add = { context: f.owner, mutationId: 'old_remember', record: record() };
  await f.store.remember(add);
  f.store.delete({ context: f.owner, mutationId: 'delete_a', record: { id: 'record_a', expectedRevision: 1 } });
  assert.throws(() => f.open({ databasePath: ':memory:' }), error('memory_restore_reconciliation_required'));
  assert.throws(() => f.open({ databasePath: join(f.directory, 'lost-local-replacement.sqlite') }), error('memory_restore_reconciliation_required'));
  assert.equal((await f.store.remember(add)).replayed, true);
  assert.equal(f.store.export({ context: f.owner }).records.length, 0);
});
