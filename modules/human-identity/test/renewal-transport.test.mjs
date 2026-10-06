import test from 'node:test';
import assert from 'node:assert/strict';
import { performance } from 'node:perf_hooks';
import { environment } from './support/renewal-fixture.mjs';

test('production transport lifetime survives a quota-like synchronous pause and one real token request without retry', { timeout: 20000 }, async t => {
  const f = await environment(t); await f.login(f.rps[0], f.actor, f.wire, undefined, 86400);
  const response = await f.wire.request(f.issuer + '/.well-known/openid-configuration');
  assert.equal(response.status, 200); assert.equal(response.headers.get('keep-alive')?.includes('timeout=65'), true);
  const token = f.rps[0].verificationFixture().refreshToken;
  // Monotonic time is deliberate: no token clocks/expiry or quota limits change.
  const deadline = performance.now() + 6100; while (performance.now() < deadline) { /* simulate synchronous SQLite/fsync work */ }
  const refreshed = await f.refresh(f.rps[0], token); assert.equal(refreshed.status, 200);
  assert.equal(typeof refreshed.body.refresh_token, 'string');
});
