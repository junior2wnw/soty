import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import { createOAuthIngress } from '../capabilities-oauth-ingress.js';
import { createUserinfoBudgetAuthority } from '../userinfo-budget-authority.js';

// The classifier here is a deterministic HOST fixture, not authentication.
// Real encrypted AT / signed Connect / SDK proof is exercised by the identity suite.
const hash = value => createHash('sha256').update(value).digest('hex');
function fixture() {
  const authority = createUserinfoBudgetAuthority(token => ({ key: hash(token), pin: hash('pin:' + token) }));
  const guard = createOAuthIngress({ userinfoBudget: authority });
  const enter = index => {
    const req = { method: 'GET', originalUrl: '/human-identity/userinfo', rawHeaders: ['authorization',
      'Bearer budget_fixture_' + String(index).padStart(8, '0')], socket: { remoteAddress: 'actual-shared-socket-peer' } };
    return guard.enter(req, {}, authority.capture(req));
  };
  return { enter };
}

test('exact2048 default rate slots refuse a new live key without evicting any old one; fixed-step expiry releases bounded capacity', t => {
  let clock = 1000; t.mock.method(performance, 'now', () => clock);
  const f = fixture();
  for (let index = 0; index < 2048; index++) f.enter(index).release();
  assert.throws(() => f.enter(2048), error => error.code === 'temporarily_unavailable');
  f.enter(0).release(); f.enter(2047).release();
  clock += 60001;
  // Capacity is recovered at the public enter boundary, not by reading its Map.
  f.enter(2048).release(); f.enter(0).release();
});

test('exact9600 default host ceiling covers all authenticated keys on a raw shared peer, then resets only after the fixed window', t => {
  let clock = 1000; t.mock.method(performance, 'now', () => clock);
  const f = fixture();
  for (let index = 0; index < 9600; index++) f.enter(index % 2048).release();
  assert.throws(() => f.enter(0), error => error.code === 'rate_limit');
  clock += 59999; assert.throws(() => f.enter(2047), error => error.code === 'rate_limit');
  clock += 1; f.enter(0).release();
});
