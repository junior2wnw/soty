import test from 'node:test';
import assert from 'node:assert/strict';
import { parseHumanLoginContext } from './human-context.mjs';
const interactionId = 'fixture_interaction_123', checkedAt = 1000000;
const context = () => ({ schema: 'soty.human-login-context.v1', interactionId, browserNonce: 'b'.repeat(43), csrf: 'c'.repeat(43),
  client: { id: 'independent-app', label: 'Независимое приложение' }, scopes: ['openid', 'profile'], expiresAt: checkedAt + 600000, decision: 'pending' });
const parse = value => parseHumanLoginContext(value, { interactionId, checkedAt });
test('human login context keeps identity tokens, redirect parameters and excessive scopes out of the UI', () => {
  for (const extra of [{ sub: 'foreign' }, { access_token: 'PRIVATE-SENTINEL' }, { redirect_uri: 'https://elsewhere.test' }, { clientSecret: 'PRIVATE-SENTINEL' }]) {
    assert.throws(() => parse({ ...context(), ...extra }), error => error.message === 'human_login_context_unavailable');
  }
  for (const scopes of [[], ['profile'], ['openid', 'openid'], ['openid', 'offline_access'], ['openid', 'email']]) assert.throws(() => parse({ ...context(), scopes }));
});
test('stale, different browser interaction and malformed display text cannot enable a decision', () => {
  for (const changed of [{ interactionId: 'foreign_interaction_123' }, { browserNonce: '../elsewhere' }, { csrf: '' },
    { expiresAt: checkedAt }, { expiresAt: checkedAt + 601001 }, { decision: 'ready' },
    { client: { id: 'independent-app', label: 'bad\u0000label' } }]) assert.throws(() => parse({ ...context(), ...changed }));
  const value = parse(context()); assert(Object.isFrozen(value) && Object.isFrozen(value.client) && Object.isFrozen(value.scopes));
  assert.equal(value.remainingMs, 600000);
});
