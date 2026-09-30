import test from 'node:test';
import assert from 'node:assert/strict';
import { fixture, principal, connection, error, deferred, settle } from './test-support/access-panel.mjs';

test('equal connection descriptions and colliding short codes retain a visible exact revoke subject', async t => {
  const firstId = 'oauth_11111111-1111-4111-8111-123456789abc';
  const secondId = 'oauth_22222222-2222-4222-8222-123456789abc';
  assert.equal(firstId.slice(-12), secondId.slice(-12));
  const f = fixture(t, { connections: [connection(firstId), connection(secondId)] }); await settle();
  const details = f.host.querySelectorAll('.sa-connection-details');
  assert.equal(details.length, 2);
  for (const id of [firstId, secondId]) {
    const detail = details.find(node => node.dataset.saConnection === id);
    assert.ok(detail.textContent.includes('Код подключения'), 'The exact reference must be labelled for the person');
    assert.ok(detail.textContent.includes(id), 'A short suffix alone does not identify this connection');
  }

  // A real refresh can be pending while an old visible card opens its dialog.
  const pending = deferred(); f.handlers.set('oauth.connections.list', () => pending.promise);
  f.button('Обновить доступы').click(); await settle();
  f.target(`revoke-connection-${secondId}`).click();
  const dialog = f.dialog();
  assert.ok(dialog.textContent.includes('Код подключения'));
  assert.ok(dialog.textContent.includes(secondId)); assert.ok(!dialog.textContent.includes(firstId));
  pending.resolve({ connections: [...f.data.connections].reverse(), nextCursor: null }); await settle();
  assert.equal(f.dialog(), dialog); assert.ok(dialog.textContent.includes(secondId));
  f.button('Отключить доступ', dialog).click(); await settle();
  assert.deepEqual(Array.from(f.writes(), call => [call.method, call.args.connectionId]),
    [['oauth.connections.revoke', secondId]]);
  assert.ok(f.target(`revoke-connection-${firstId}`));
  assert.equal(f.target(`revoke-connection-${secondId}`), undefined);
  assert.equal(f.data.connections.find(item => item.id === firstId).active, true);
});

test('unavailable details distinguish equal marked principals without pretending they are connection IDs', async t => {
  const firstId = 'principal_11111111-1111-4111-8111-123456789abc';
  const secondId = 'principal_22222222-2222-4222-8222-123456789abc';
  const f = fixture(t, {
    principals: [principal(firstId, 'oauth'), principal(secondId, 'oauth')],
    handlers: { 'oauth.connections.list': () => { throw error('oauth_unavailable'); } },
  }); await settle();
  const cards = f.host.querySelectorAll('.sa-connection-fallback'); assert.equal(cards.length, 2);
  for (const id of [firstId, secondId]) {
    const card = cards.find(node => node.textContent.includes(id)); assert.ok(card);
    assert.ok(card.textContent.includes('Код доступа')); assert.ok(!card.textContent.includes('Код подключения'));
  }
  f.target(`revoke-connection-principal-${secondId}`).click();
  const dialog = f.dialog();
  assert.ok(dialog.textContent.includes('Код доступа')); assert.ok(dialog.textContent.includes(secondId));
  assert.ok(!dialog.textContent.includes(firstId)); assert.ok(!dialog.textContent.includes('Код подключения'));
  f.button('Отключить доступ', dialog).click(); await settle();
  assert.deepEqual(Array.from(f.writes(), call => [call.method, call.args.principalId]),
    [['access.principals.revoke', secondId]]);
  assert.equal(f.calls.filter(call => /grants\.|credentials\./u.test(call.method)).length, 0);
  assert.equal(f.data.principals.find(item => item.id === firstId).state, 'active');
});
