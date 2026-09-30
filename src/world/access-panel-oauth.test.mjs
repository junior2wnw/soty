import test from 'node:test';
import assert from 'node:assert/strict';
import { fixture, principal, connection, error, deferred, settle, time } from './test-support/access-panel.mjs';

test('exact managed marker separates equal labels; two independent connections are not merged', async t => {
  const f = fixture(t, { connections: [connection('one'), connection('two'), connection('unknown_profile', 'constructor')] }); await settle();
  assert.equal(f.host.querySelectorAll('.sa-client').length, 4);
  assert.equal(f.host.querySelectorAll('.sa-connection-details').length, 3);
  assert.match(f.host.textContent, /Внешнее приложение/); assert.doesNotMatch(f.host.textContent, /native code|function Object/u);
  assert.ok(f.target('principal-key_one')); assert.equal(f.target('principal-oauth_one'), undefined);
  assert.equal(f.calls.filter(call => call.method === 'access.grants.list').length, 0);
  assert.match(f.host.textContent, /Подключённые приложения/); assert.match(f.host.textContent, /Доступ по ключу/);
  f.button('Действия').click(); await settle(); assert.equal(f.calls.filter(call => call.method === 'access.invocations.list').length, 1);
  f.button('История доступов').click(); await settle(); assert.equal(f.calls.filter(call => call.method === 'access.events.list').length, 1);
});

test('unavailable connection API leaves exact marked-principal safety revoke, without grant or secret calls', async t => {
  const f = fixture(t, { handlers: { 'oauth.connections.list': () => { throw error('oauth_unavailable'); } } }); await settle();
  const fallback = f.host.querySelector('.sa-connection-fallback'); assert.ok(fallback); assert.match(fallback.textContent, /Codex CLI/);
  f.button('Отключить доступ', fallback).click(); f.button('Отключить доступ', f.dialog()).click(); await settle();
  assert.deepEqual(f.writes().map(call => [call.method, call.args.principalId]), [['access.principals.revoke', 'oauth_one']]);
  assert.equal(f.calls.filter(call => /grants\.|credentials\./u.test(call.method)).length, 0);
  assert.equal(f.dialog(), null); assert.match(f.host.querySelector('.sa-connection-fallback').textContent, /Доступ отключён/);
  assert.equal(f.document.activeElement, f.target('connection-principal-oauth_one'));
});

test('lost revoke ACK reads the same connection; confirmed readback never sends a second revoke', async t => {
  const f = fixture(t); await settle();
  f.handlers.set('oauth.connections.revoke', args => { const item = f.data.connections.find(row => row.id === args.connectionId); item.revokedAt = time + 1; item.active = false; throw new TypeError('Synthetic lost response'); });
  f.target('revoke-connection-connection_one').click(); const confirm = f.button('Отключить доступ', f.dialog()); confirm.focus(); confirm.click(); await settle();
  assert.equal(f.writes().length, 1); assert.equal(f.document.activeElement, confirm); assert.match(f.dialog().textContent, /не подтверждено/);
  f.button('Проверить подключение', f.dialog()).click(); await settle();
  assert.equal(f.writes().length, 1); assert.equal(f.dialog(), null); assert.equal(f.target('revoke-connection-connection_one'), undefined);
});

test('bounded readback that misses the subject permits only an explicit repeat of its captured ID', async t => {
  const f = fixture(t); await settle();
  f.handlers.set('oauth.connections.revoke', () => { throw new TypeError('Synthetic timeout'); });
  f.target('revoke-connection-connection_one').click(); f.button('Отключить доступ', f.dialog()).click(); await settle();
  f.handlers.set('oauth.connections.list', () => ({ connections: [connection('other')], nextCursor: 'more' }));
  const before = f.calls.length; f.button('Проверить подключение', f.dialog()).click(); await settle();
  assert.equal(f.calls.length, before + 1); assert.equal(f.writes().length, 1); assert.match(f.dialog().textContent, /не найдено/);
  f.handlers.set('oauth.connections.revoke', args => ({ connectionId: args.connectionId, revoked: true }));
  f.button('Повторить отключение', f.dialog()).click(); await settle();
  assert.deepEqual(f.writes().map(call => call.args.connectionId), ['connection_one', 'connection_one']);
});

test('wrong or coercible revoke receipts never acknowledge the action', async t => {
  for (const response of [{ connectionId: ['connection_one'], revoked: true }, { connectionId: 'other', revoked: true }, { connectionId: 'connection_one', revoked: 'true' }]) {
    const f = fixture(t); await settle(); f.handlers.set('oauth.connections.revoke', () => response);
    f.target('revoke-connection-connection_one').click(); f.button('Отключить доступ', f.dialog()).click(); await settle();
    assert.ok(f.button('Проверить подключение', f.dialog())); assert.equal(f.host.querySelector('.sa-announcement').textContent, '');
    f.handle.dispose();
  }
});

test('valid ACK invalidates a read started before it; unknown result survives close/reopen as readback', async t => {
  const f = fixture(t); await settle(); const old = deferred();
  f.handlers.set('oauth.connections.list', () => old.promise); f.button('Обновить доступы').click(); await settle();
  f.target('revoke-connection-connection_one').click(); f.button('Отключить доступ', f.dialog()).click(); await settle();
  old.resolve({ connections: [connection('connection_one')], nextCursor: null }); await settle();
  assert.equal(f.target('revoke-connection-connection_one'), undefined); assert.match(f.host.textContent, /отключён/);
  const g = fixture(t); await settle(); g.handlers.set('oauth.connections.revoke', () => { throw new TypeError('Synthetic unknown'); });
  g.target('revoke-connection-connection_one').click(); g.button('Отключить доступ', g.dialog()).click(); await settle();
  g.button('Закрыть', g.dialog().querySelector('.sa-dialog-actions')).click(); g.target('revoke-connection-connection_one').click();
  assert.ok(g.button('Проверить подключение', g.dialog())); assert.equal(g.writes().length, 1);
});

test('native account-admission failure erases both lists and late projection cannot restore them', async t => {
  const principals = deferred(), connections = deferred();
  const f = fixture(t, { handlers: { 'access.principals.list': () => principals.promise, 'oauth.connections.list': () => connections.promise } });
  principals.reject(error('ACTIVE_PROFILE_CHANGED')); await settle(); assert.match(f.host.textContent, /Доступ к аккаунту изменился/);
  connections.resolve({ connections: [connection('private_late')], nextCursor: null }); await settle();
  assert.equal(f.host.querySelectorAll('.sa-client').length, 0); assert.doesNotMatch(f.host.textContent, /private|Codex/u);
  assert.equal(f.dialog(), null);
});

test('late revoke ACK after dispose cannot repaint another account or move its focus', async t => {
  const f = fixture(t); await settle(); const ack = deferred(); f.handlers.set('oauth.connections.revoke', () => ack.promise);
  f.target('revoke-connection-connection_one').click(); f.button('Отключить доступ', f.dialog()).click();
  f.handle.dispose(); f.host.textContent = 'Other account'; f.outside.focus();
  ack.resolve({ connectionId: 'connection_one', revoked: true }); await settle();
  assert.equal(f.host.textContent, 'Other account'); assert.equal(f.document.activeElement, f.outside); assert.equal(f.dialog(), null);
});

test('marked-only principal pages keep navigation; connection paging is independent', async t => {
  const marked = Array.from({ length: 20 }, (_, index) => principal(`marked_${index}`, 'oauth'));
  const f = fixture(t, { handlers: {
    'access.principals.list': args => ({ principals: args.cursor ? [principal('later_key')] : marked, cursor: args.cursor ? null : 'principal_page2' }),
    'oauth.connections.list': args => ({ connections: args.cursor ? [connection('later_connection')] : Array.from({ length: 20 }, (_, index) => connection(`connection_${index}`)), nextCursor: args.cursor ? null : 'connection_page2' }),
  } }); await settle();
  assert.match(f.host.textContent, /На этой странице нет доступов по ключу/);
  f.target('page-clients-next').click(); await settle(); assert.ok(f.target('principal-later_key'));
  assert.equal(f.calls.filter(call => call.method === 'oauth.connections.list').length, 1);
  f.target('page-connections-next').click(); await settle(); assert.ok(f.target('connection-connection-later_connection'));
  assert.equal(f.calls.filter(call => call.method === 'access.principals.list').length, 2);
});

test('connection disclosures and logical focus survive refresh; busy controls cannot repeat or close a revoke', async t => {
  const f = fixture(t); await settle(); f.host.querySelector('.sa-connection-details').open = true;
  f.button('Обновить доступы').focus(); f.button('Обновить доступы').click(); await settle();
  assert.equal(f.host.querySelector('.sa-connection-details').open, true);
  assert.equal(f.document.activeElement, f.button('Обновить доступы'));
  const ack = deferred(); f.handlers.set('oauth.connections.revoke', () => ack.promise);
  f.target('revoke-connection-connection_one').click(); const confirm = f.button('Отключить доступ', f.dialog()); confirm.focus(); confirm.click(); confirm.click();
  assert.equal(confirm.disabled, false); assert.equal(confirm.getAttribute('aria-disabled'), 'true'); assert.equal(f.writes().length, 1);
  assert.equal(f.dialog().dispatch('keydown', { key: 'Escape' }).defaultPrevented, true);
  f.button('Закрыть', f.dialog().querySelector('.sw-dialog-header')).click(); assert.ok(f.dialog());
  f.outside.focus(); ack.resolve({ connectionId: 'connection_one', revoked: true }); await settle();
  assert.equal(f.document.activeElement, f.outside, 'late completion must not steal focus from a later choice');
});

test('malformed connection projection fails into marked fallback; legacy key access still uses its own flow', async t => {
  const f = fixture(t, { handlers: { 'oauth.connections.list': () => ({ connections: [{ ...connection('bad'), id: ['bad'] }], nextCursor: null }) } }); await settle();
  assert.equal(f.host.querySelectorAll('.sa-connection-details').length, 0); assert.ok(f.host.querySelector('.sa-connection-fallback'));
  f.target('principal-key_one').click(); await settle(); f.target('revoke-principal-key_one').click(); await settle();
  f.button('Отключить клиента', f.dialog()).click(); await settle();
  assert.deepEqual(f.writes().map(call => [call.method, call.args.principalId]), [['access.principals.revoke', 'key_one']]);
});
