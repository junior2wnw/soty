import test from 'node:test';
import assert from 'node:assert/strict';
import { createAppCreateState } from './app-create-state.mjs';

const accountId = 'account_alice';
const input = { expectedAccountId: accountId, hostDeviceId: 'host:alice', connectorId: 'connector:one', text: 'Create a private app', cwd: 'C:\\projects\\one' };
const job = { hostDeviceId: input.hostDeviceId, connectorId: input.connectorId, jobId: 'job_created' };
function fixture() {
  const values = new Map(); let writesFail = false, readsFail = false, sequence = 0;
  const storage = { getItem: key => { if (readsFail) throw new Error('storage_unavailable'); return values.get(key) ?? null; },
    setItem: (key, value) => { if (writesFail) throw new Error('quota'); values.set(key, value); } };
  const queues = new Map(), lockNames = [];
  const locks = { request(key, action) { lockNames.push(key); const next = (queues.get(key) ?? Promise.resolve()).then(action);
    queues.set(key, next.catch(() => undefined)); return next; } };
  const open = (selectedAccount = accountId, extra = {}) => createAppCreateState({ accountId: selectedAccount, storage, locks, randomId: () => `request_${++sequence}`, ...extra });
  return { values, storage, locks, lockNames, open, failWrites: value => { writesFail = value; }, failReads: value => { readsFail = value; } };
}

test('lost create ACK survives close/reload with exactly the same intent and immutable device pair', async () => {
  const f = fixture(), first = f.open(), pending = await first.prepare(input);
  const reopened = f.open();
  assert.deepEqual(reopened.read().pending, pending);
  assert.deepEqual(await reopened.prepare(input), pending);
  await assert.rejects(reopened.prepare({ ...input, hostDeviceId: 'another_host' }), /app_create_pending_unconfirmed/u);
  assert.deepEqual(reopened.read().pending, pending);
  const copy = reopened.read(); copy.pending.payload.cwd = 'changed'; copy.draft.text = 'changed';
  assert.equal(reopened.read().pending.payload.cwd, input.cwd);
  assert.equal(reopened.read().draft.text, input.text);
});

test('two tabs serialize prepare; a competing intent cannot create a second unconfirmed request', async () => {
  const f = fixture(), first = f.open(), second = f.open();
  const [a, b] = await Promise.all([first.prepare(input), second.prepare(input)]);
  assert.equal(a.payload.requestId, b.payload.requestId);
  await assert.rejects(second.prepare({ ...input, text: 'another app' }), /app_create_pending_unconfirmed/u);
  assert.ok(f.lockNames.every(value => value === first.key));
});

test('a matching accepted receipt is durable after reload, and a newer draft is not erased', async () => {
  const f = fixture(), first = f.open(), second = f.open(), pending = await first.prepare(input);
  await second.saveDraft({ text: 'Next app, not yet submitted', cwd: 'C:\\next' });
  assert.equal(await first.acknowledge(pending.payload, job), true);
  const restored = f.open().read();
  assert.deepEqual(restored.accepted, job); assert.equal(restored.pending, null);
  assert.equal(restored.draft.text, 'Next app, not yet submitted'); assert.equal(restored.draft.cwd, 'C:\\next');
  await assert.rejects(first.prepare(input), /app_create_result_pending/u);
  assert.equal(await first.clearAccepted({ ...job, jobId: 'other_job' }), false);
  assert.equal(await first.clearAccepted(job), true);
  const next = await first.prepare({ ...input, text: restored.draft.text, cwd: restored.draft.cwd });
  assert.notEqual(next.payload.requestId, pending.payload.requestId);
  assert.equal(await second.acknowledge(pending.payload, job), false, 'late old receipt cannot replace the next request');
  assert.equal(first.read().pending.payload.requestId, next.payload.requestId);
});

test('receipt requires exact intent, account and target, not merely a plausible job ID', async () => {
  const f = fixture(), state = f.open(), pending = await state.prepare(input);
  assert.equal(await state.acknowledge({ ...pending.payload, cwd: 'C:\\elsewhere' }, job), false);
  assert.equal(await state.acknowledge({ ...pending.payload, expectedAccountId: 'account_bob' }, job), false);
  await assert.rejects(state.acknowledge(pending.payload, { ...job, connectorId: 'other_connector' }), /invalid_app_create_receipt/u);
  assert.deepEqual(state.read().pending, pending);
  assert.equal(await state.acknowledge(pending.payload, job), true);
  assert.equal(state.read().draft.text, '', 'only the exact unchanged draft is cleared');
});

test('only the exact durable rejected receipt unlocks the original draft for another attempt', async () => {
  const f = fixture(), state = f.open(), pending = await state.prepare(input);
  await assert.rejects(state.reject(pending.payload, { status: 'rejected', requestId: 'wrong_request', reason: 'app_model_unavailable' }), /invalid_app_create_receipt/u);
  await assert.rejects(state.reject(pending.payload, { status: 'unknown', requestId: pending.payload.requestId, reason: 'app_model_unavailable' }), /invalid_app_create_receipt/u);
  assert.deepEqual(state.read().pending, pending);
  assert.equal(await state.reject(pending.payload, { status: 'rejected', requestId: pending.payload.requestId, reason: 'app_model_unavailable' }), true);
  assert.equal(state.read().pending, null); assert.equal(state.read().draft.text, input.text);
  const next = await state.prepare(input); assert.notEqual(next.payload.requestId, pending.payload.requestId);
  assert.equal(await state.reject(pending.payload, { status: 'rejected', requestId: pending.payload.requestId, reason: 'app_model_unavailable' }), false);
});

test('quota blocks dispatch, retains the visible draft and failed ACK storage preserves recoverable pending', async () => {
  const f = fixture(), state = f.open(); f.failWrites(true);
  await assert.rejects(state.saveDraft({ text: input.text, cwd: input.cwd, hostDeviceId: input.hostDeviceId, connectorId: input.connectorId }), /app_create_storage_unavailable/u);
  assert.equal(state.read().draft.text, input.text); assert.equal(state.hasUnsavedChanges(), true);
  await assert.rejects(state.prepare(input), /app_create_storage_unavailable/u);
  f.failWrites(false); await state.flush(); assert.equal(state.hasUnsavedChanges(), false);
  const pending = await state.prepare(input); f.failWrites(true);
  await assert.rejects(state.acknowledge(pending.payload, job), /app_create_storage_unavailable/u);
  f.failWrites(false);
  assert.deepEqual(f.open().read().pending, pending, 'failed ACK storage must never generate another intent');
  assert.equal(await state.acknowledge(pending.payload, job), true);
});

test('corrupted or foreign saved intent fails closed without overwriting its raw recovery record', async () => {
  const f = fixture(), state = f.open(), pending = await state.prepare(input);
  const bad = JSON.parse(f.values.get(state.key)); bad.pending.payload.expectedAccountId = 'account_bob';
  const raw = JSON.stringify(bad); f.values.set(state.key, raw);
  assert.equal(f.open().hasUnsavedChanges(), false, 'read is what validates stored data');
  const reopened = f.open(); reopened.read(); assert.equal(reopened.hasUnsavedChanges(), true);
  await assert.rejects(reopened.prepare(input), /app_create_storage_unavailable/u);
  assert.equal(f.values.get(state.key), raw);
  assert.equal(f.open('account_bob').read().pending, null);
  await assert.rejects(state.prepare({ ...input, expectedAccountId: 'account_bob' }), /invalid_account/u);
  assert.ok(pending.payload.requestId);
});

test('known legacy results may be adopted but never replace an unconfirmed or newer known task', async () => {
  const f = fixture(), state = f.open(); assert.equal(await state.adoptAccepted(job), true);
  assert.equal(await state.adoptAccepted({ ...job, jobId: 'other_job' }), false);
  assert.deepEqual(f.open().read().accepted, job);
  await state.clearAccepted(job); const pending = await state.prepare(input);
  assert.equal(await state.adoptAccepted(job), false); assert.deepEqual(state.read().pending, pending);
});

test('a missing Web Locks implementation permits recovery and drafts, but never new dispatch', async () => {
  const f = fixture(), state = f.open(accountId, { locks: undefined });
  await state.saveDraft({ text: 'Keep this in the browser' });
  assert.equal(state.canDispatch(), false); assert.equal(state.read().draft.text, 'Keep this in the browser');
  await assert.rejects(state.prepare(input), /app_create_lock_unavailable/u);
  assert.equal(state.read().pending, null);
});

test('read failures cannot replace a pending request, and tuple-like device values stay distinct', async () => {
  const f = fixture(), state = f.open(); const pending = await state.prepare({ ...input, hostDeviceId: 'host:part', connectorId: 'connector' });
  f.failReads(true); state.read(); await assert.rejects(state.prepare(input), /app_create_storage_unavailable/u);
  f.failReads(false); assert.deepEqual(state.read().pending, pending);
  await assert.rejects(state.prepare({ ...input, hostDeviceId: 'host', connectorId: 'part:connector' }), /app_create_pending_unconfirmed/u);
});

test('an edit returning to the same text is still a later draft, not cleared by the old receipt', async () => {
  const f = fixture(), state = f.open(), pending = await state.prepare(input);
  await state.saveDraft({ text: 'A newer idea' }); await state.saveDraft({ text: input.text });
  await state.acknowledge(pending.payload, job);
  assert.equal(state.read().draft.text, input.text);
});

test('explicit discard removes only volatile edits, preserving another tab and the durable pending', async () => {
  const f = fixture(), first = f.open(), second = f.open(), pending = await first.prepare(input);
  f.failWrites(true); await assert.rejects(first.saveDraft({ text: 'Unsaved local change' }), /app_create_storage_unavailable/u);
  assert.equal(first.hasVolatileDraft(), true);
  f.failWrites(false); await second.saveDraft({ text: 'A saved change from another tab' });
  first.discardLocalDraft();
  assert.equal(first.hasVolatileDraft(), false); assert.equal(first.read().draft.text, 'A saved change from another tab');
  assert.deepEqual(first.read().pending, pending);
});

test('input is guarded before any identity await, and delayed earlier saves cannot win', async () => {
  const f = fixture(), state = f.open();
  const earlier = state.stageDraft({ text: 'First input awaiting identity' });
  assert.equal(state.hasVolatileDraft(), true, 'close and beforeunload see input synchronously');
  assert.equal(state.hasUnsavedChanges(), true, 'PWA cannot call the unsaved input safe');
  const later = state.stageDraft({ text: 'Latest input while first identity check waits' });
  await state.persistDraft(later);
  await state.persistDraft(earlier);
  assert.equal(f.open().read().draft.text, 'Latest input while first identity check waits');
  assert.equal(state.hasVolatileDraft(), false);
});

test('update flush stores staged input and explicit discard fences an already queued save', async () => {
  const f = fixture(), state = f.open(); state.stageDraft({ text: 'Input then immediate update' });
  await state.flush(); assert.equal(f.open().read().draft.text, 'Input then immediate update');
  const staged = state.stageDraft({ text: 'Will be explicitly discarded' });
  state.discardLocalDraft(); await state.persistDraft(staged);
  assert.equal(f.open().read().draft.text, 'Input then immediate update');
});
