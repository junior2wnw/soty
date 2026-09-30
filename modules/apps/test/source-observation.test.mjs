import test from 'node:test';
import assert from 'node:assert/strict';
import { describeSourceObservation } from '../server/source-observation.mjs';

test('a responding source becomes unknown at the exact45s boundary, retaining its last evidence', () => {
  const observed = { state: 'ready', at: 1_000_000 };
  assert.equal(describeSourceObservation({ connected: true, observed, now: 1_044_999 }).state, 'responding');
  assert.deepEqual(describeSourceObservation({ connected: true, observed, now: 1_045_000 }),
    { state: 'unknown', observedAt: 1_000_000, freshUntil: 1_045_000, evidence: 'connector-v1-observation' });
  assert.equal(describeSourceObservation({ connected: true, observed, now: 1_400_000 }).state, 'unknown');
});

test('an offline connector and a reconnected unobserved source cannot reuse former readiness', () => {
  const observed = { state: 'ready', at: 1_000_000 };
  assert.deepEqual(describeSourceObservation({ connected: false, observed, now: 1_000_001 }),
    { state: 'offline', observedAt: null, freshUntil: null, evidence: 'connector-offline' });
  assert.deepEqual(describeSourceObservation({ connected: true, now: 1_000_002 }),
    { state: 'unknown', observedAt: null, freshUntil: null, evidence: 'not-observed' });
});

test('failure reports also expire; malformed or future clocks never advertise readiness', () => {
  assert.equal(describeSourceObservation({ connected: true, observed: {state:'stopped',at:1000}, now:1001 }).state, 'unreachable');
  assert.equal(describeSourceObservation({ connected: true, observed: {state:'stopped',at:1000}, now:46000 }).state, 'unknown');
  for (const observed of [{state:'ready',at:2000},{state:'ready',at:NaN},{state:'ready',at:-1},{state:'ready',at:Infinity},{state:'ready',at:1.5},{state:'online',at:1}]) {
    const result = describeSourceObservation({ connected: true, observed, now:1000 });
    assert.equal(result.state, 'unknown'); assert.equal(result.observedAt, null);
  }
});

test('a v2 HEAD keeps its precise evidence and expires without becoming a legacy or functional claim', () => {
  const observed = { state: 'ready', at: 1000, evidence: 'connector-v2-observation' };
  assert.deepEqual(describeSourceObservation({ connected: true, observed, now: 1001 }),
    { state: 'responding', observedAt: 1000, freshUntil: 46000, evidence: 'connector-v2-observation' });
  assert.deepEqual(describeSourceObservation({ connected: true, observed, now: 46000 }),
    { state: 'unknown', observedAt: 1000, freshUntil: 46000, evidence: 'connector-v2-observation' });
});
