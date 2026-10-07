import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import { getEventListeners } from 'node:events';
import { performance } from 'node:perf_hooks';
import { environment, RECEIPT, BODY, fence, sha } from './synthetic-fixture.mjs';

const completeBinding = async env => {
  await env.client.warmBeforeServingStop();
  await env.client.bindInsideAuthenticatedHook(RECEIPT, fence(RECEIPT)).catch(() => {});
};

test('native abort from body-start telemetry denies dispatch and performs no payload read', async t => {
  let env, pulls = 0, events = 0;
  const payload = new Readable({ highWaterMark: 65536, read() { pulls++; } });
  env = environment({ payloadInput: payload, emit(record) {
    if (record.phase === 'body_receiver_start') { events++; env.abort.abort(); }
  } });
  t.after(env.close); await completeBinding(env);
  await assert.rejects(env.rawRun, { code: 'warm_cancelled' });
  assert.equal(events, 1); assert.equal(env.counts.receiveCalls, 0); assert.equal(pulls, 0);
  assert.equal(env.counts.closed, 1); assert.equal(env.payload.destroyed, true);
  assert.equal(env.phases.some(record => record.phase === 'physical_receipt'), false);
  assert.equal(getEventListeners(env.abort.signal, 'abort').length, 0);
});

test('absolute lease expiring inside synchronous telemetry denies dispatch before timer can run', async t => {
  let events = 0, callbackElapsed = 0;
  // This is a bounded synthetic driver lease, not a changed sender/body clock.
  const env = environment({ leaseMs: 400, clientLeaseMs: 2000, emit(record) {
    if (record.phase !== 'body_receiver_start') return;
    events++; const started = performance.now();
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 430);
    callbackElapsed = performance.now() - started;
  } });
  t.after(env.close); await completeBinding(env);
  await assert.rejects(env.rawRun, { code: 'warm_lease_expired' });
  assert.equal(events, 1); assert.ok(callbackElapsed >= 400);
  assert.equal(env.counts.receiveCalls, 0); assert.equal(env.counts.closed, 1);
  assert.equal(env.phases.some(record => record.phase === 'physical_receipt'), false);
});

test('synchronous reentry plus native abort and private telemetry throw cannot dispatch or overwrite cancellation', async t => {
  let env, nested, reads = 0;
  const poison = {};
  for (const key of ['message', 'code', 'stack', 'reason']) Object.defineProperty(poison, key, {
    get() { reads++; throw Error('test_private_getter_must_not_run'); },
  });
  env = environment({ emit(record) {
    if (record.phase !== 'body_receiver_start') return;
    nested = env.client.bindInsideAuthenticatedHook(RECEIPT, fence(RECEIPT)).then(
      () => 'unexpected_success', error => error.code);
    env.abort.abort(poison); throw poison;
  } });
  t.after(env.close); await completeBinding(env);
  await assert.rejects(env.rawRun, { code: 'warm_cancelled' });
  assert.equal(await nested, 'warm_stage_invalid'); assert.equal(reads, 0);
  assert.equal(env.counts.receiveCalls, 0); assert.equal(env.counts.closed, 1);
  assert.equal(env.phases.some(record => record.phase === 'physical_receipt'), false);
});

test('final fence uses native aborted state without reading shadow signal or private reason getters', async t => {
  let env, reads = 0;
  env = environment({ emit(record) { if (record.phase === 'body_receiver_start') env.abort.abort(); } });
  for (const key of ['aborted', 'reason']) Object.defineProperty(env.abort.signal, key, {
    get() { reads++; throw Error('test_signal_getter_must_not_run'); },
  });
  t.after(env.close); await completeBinding(env);
  await assert.rejects(env.rawRun, { code: 'warm_cancelled' });
  assert.equal(env.counts.receiveCalls, 0); assert.equal(reads, 0);
});

test('telemetry exception alone preserves valid physical result and reads no private fields', async t => {
  let reads = 0;
  const poison = {};
  for (const key of ['message', 'code', 'stack', 'reason']) Object.defineProperty(poison, key, {
    get() { reads++; throw Error('test_private_getter_must_not_run'); },
  });
  const env = environment({ emit(record) { if (record.phase === 'body_receiver_start') throw poison; } });
  t.after(env.close); await env.client.warmBeforeServingStop();
  await env.client.bindInsideAuthenticatedHook(RECEIPT, fence(RECEIPT)); env.payload.end(BODY);
  const physical = await env.rawRun;
  assert.equal(physical.plaintextSha256, sha(BODY)); assert.equal(physical.readbackVerified, true);
  assert.equal(env.counts.receiveCalls, 1); assert.equal(reads, 0); assert.equal(env.counts.closed, 1);
});
