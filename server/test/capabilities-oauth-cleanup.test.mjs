import test from 'node:test';
import assert from 'node:assert/strict';
import { startOAuthCleanup } from '../capabilities-oauth-cleanup.js';

function clock() {
  const pending = new Set(), history = []; let detached = 0;
  return { pending, history, get detached() { return detached; },
    timers: { setTimeout(fn, delay) { const timer = { fn, delay, unref() { detached++; } }; pending.add(timer); history.push(delay); return timer; },
      clearTimeout(timer) { pending.delete(timer); } },
    run() { assert.equal(pending.size, 1); const timer = [...pending][0]; pending.delete(timer); timer.fn(); return timer.fn; },
  };
}

test('housekeeping takes one shared bounded page per turn, detaches timers and does not loop over full pages', () => {
  const c = clock(), requests = [];
  const worker = startOAuthCleanup({ timers: c.timers, oauth: { cleanup(args) {
    requests.push(args); return { artifactsDeleted: 30, interactionsDeleted: 30, credentialsDeleted: 4 };
  } } });
  assert.deepEqual(c.history, [10000]); assert.equal(c.detached, 1); assert.equal(requests.length, 0);
  const late = c.run();
  assert.deepEqual(requests, [{ limit: 64 }]); assert.deepEqual(c.history, [10000, 30000]);
  assert.equal(worker.status().deleted, 64); assert.equal(worker.status().unavailable, false);
  worker.close(); worker.close(); assert.equal(c.pending.size, 0);
  late(); assert.equal(requests.length, 1); assert.equal(c.pending.size, 0);
});

test('failed, malformed or async cleanup is unavailable with fixed backoff, no content or unhandled rejection', async () => {
  for (const outcome of [new Error('private-fixture-detail'), {}, { artifactsDeleted: 65, interactionsDeleted: 0, credentialsDeleted: 0 },
    { artifactsDeleted: 40, interactionsDeleted: 25, credentialsDeleted: 0 }, () => Promise.reject(new Error('private-fixture-detail'))]) {
    const c = clock();
    const worker = startOAuthCleanup({ timers: c.timers, oauth: { cleanup() {
      if (outcome instanceof Error) throw outcome; return typeof outcome === 'function' ? outcome() : outcome;
    } } });
    c.run(); await Promise.resolve();
    assert.deepEqual(c.history, [10000, 60000]);
    assert.equal(worker.status().unavailable, true); assert.equal(worker.status().deleted, 0);
    assert.equal(JSON.stringify(worker.status()).includes('private-fixture-detail'), false); worker.close();
  }
});

test('closing before first turn prevents housekeeping; an unavailable port is rejected synchronously', () => {
  const c = clock(); let calls = 0;
  const worker = startOAuthCleanup({ timers: c.timers, oauth: { cleanup() { calls++; } } });
  const late = [...c.pending][0].fn; worker.close(); late();
  assert.equal(calls, 0); assert.equal(c.pending.size, 0);
  assert.throws(() => startOAuthCleanup({ oauth: {} }), /oauth_cleanup_configuration_invalid/);
});
