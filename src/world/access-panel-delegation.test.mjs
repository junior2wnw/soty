import test from 'node:test';
import assert from 'node:assert/strict';
import { fixture, principal, grant, audience, time, settle, deferred } from './test-support/access-panel-delegation.mjs';

const issueCalls = f => f.calls.filter(call => call.method === 'access.grants.issue');
function fill(form, allowDelegation = false) {
  const name = form.querySelectorAll('input').find(node => node.type === 'text');
  name.value = 'Клиент с помощниками'; name.dispatch('input');
  const checkbox = form.querySelector('.sa-delegation-check'); checkbox.checked = allowDelegation; checkbox.dispatch('change');
  return checkbox;
}

test('manual delegation is a labelled native opt-in; unchecked keeps the original narrow grant and one-shot secret', async t => {
  const f = fixture(t); await settle(); const form = await f.openCreate(), checkbox = form.querySelector('.sa-delegation-check');
  assert.equal(checkbox.type, 'checkbox'); assert.equal(checkbox.checked, false);
  assert.equal(checkbox.parentElement.tagName, 'LABEL'); assert.equal(checkbox.parentElement.htmlFor, checkbox.id);
  assert.match(checkbox.parentElement.textContent, /Разрешить этому клиенту подключать помощников/u);
  assert.ok(form.querySelectorAll('p').some(node => node.id === checkbox.getAttribute('aria-describedby')));
  fill(form); form.dispatch('submit'); await f.handle.flush(); await settle();
  assert.equal(issueCalls(f).length, 1);
  const issued = issueCalls(f)[0].args;
  assert.equal(issued.allowDelegation, false); assert.equal(issued.maxDepth, 0);
  assert.deepEqual(issued.capabilities, [{ capabilityId: 'notes.createDraft', version: 1 }]);
  assert.deepEqual(issued.resources, ['notes:new']); assert.deepEqual(issued.effects, ['create']); assert.deepEqual(issued.recipients, ['soty:notes']);
  assert.deepEqual(issued.budget, { unit: 'invocations', limit: 10 });
  const secret = f.dialog().querySelector('.sa-secret'); assert.ok(secret && secret.type === 'password' && secret.value.length > 0);
  assert.match(f.dialog().textContent, /один раз и не восстанавливается/u);
  assert.match(f.dialog().textContent, /ПомощникиНе разрешены/u);
  f.button('Скопировать ключ', f.dialog()).click(); await settle(); assert.equal(f.copied, 1);
  f.button('Готово, закрыть', f.dialog()).click(); assert.equal(secret.value.length, 0); assert.equal(f.dialog(), null);
  assert.equal(f.handle.hasUnsavedChanges(), false);
  assert.equal(f.calls.filter(call => call.method === 'access.credentials.issue').length, 1);
});

test('checked consent is captured before async availability and cannot follow a later checkbox value', async t => {
  const f = fixture(t); await settle(); const form = await f.openCreate(), checkbox = fill(form, true), held = deferred();
  f.controls.availability = () => held.promise;
  form.dispatch('submit'); assert.equal(checkbox.disabled, true); assert.equal(f.handle.hasUnsavedChanges(), true);
  checkbox.checked = false; checkbox.dispatch('change'); form.dispatch('submit');
  held.resolve({ notesCreateEnabled: true, audience }); await f.handle.flush(); await settle();
  assert.equal(issueCalls(f).length, 1); assert.equal(issueCalls(f)[0].args.allowDelegation, true); assert.equal(issueCalls(f)[0].args.maxDepth, 1);
  assert.equal(f.calls.filter(call => call.method === 'access.principals.create').length, 1);
  assert.match(f.dialog().textContent, /Общий лимит10 действий на всю цепочку/u);
  assert.match(f.dialog().textContent, /Разрешены · один уровень/u);
  assert.match(f.dialog().textContent, /Отзыв одного ключа не отключает уже подключённых помощников/u);
});

test('a server receipt with different delegation consent never advances to credential issuance', async t => {
  const f = fixture(t); await settle(); const form = await f.openCreate(); fill(form, false);
  f.handlers.set('access.grants.issue', args => ({ grant: grant('unexpected_grant', args.principalId, { allowDelegation: true, maxDepth: 1 }) }));
  form.dispatch('submit'); await f.handle.flush(); await settle();
  assert.equal(f.calls.filter(call => call.method === 'access.credentials.issue').length, 0);
  assert.equal(f.dialog().querySelector('.sa-secret'), null);
  assert.match(f.dialog().textContent, /Настройка не подтверждена/u);
  form.dispatch('submit'); await f.handle.flush(); assert.equal(issueCalls(f).length, 1, 'uncertain creation cannot be resubmitted inside this dialog');
});

test('lost key response is unknown, never automatically retried, and recovery revokes only the exact captured principal', async t => {
  const f = fixture(t); await settle(); const form = await f.openCreate(); fill(form, true);
  f.handlers.set('access.credentials.issue', () => { throw new TypeError('Synthetic lost acknowledgement'); });
  form.dispatch('submit'); await f.handle.flush(); await settle();
  const principalId = f.data.principals[0].id, grantId = f.data.grants[0].id, firstWrites = f.writes().length;
  assert.match(f.dialog().textContent, /Ключ мог быть выдан/u); assert.match(f.dialog().textContent, /Секрет нельзя восстановить/u);
  assert.ok(f.dialog().textContent.includes(principalId)); assert.ok(f.dialog().textContent.includes(grantId));
  assert.equal(f.dialog().querySelector('.sa-secret'), null);
  form.dispatch('submit'); await f.handle.flush(); await settle(); assert.equal(f.writes().length, firstWrites);
  f.data.principals.unshift(principal('different_principal'));
  f.handlers.set('access.principals.revoke', () => ({ principal: { ...principal('different_principal'), state: 'revoked', revokedAt: time } }));
  f.button('Закрыть созданный доступ', f.dialog()).click(); await f.handle.flush(); await settle();
  assert.match(f.dialog().textContent, /Отзыв не подтверждён/u);
  assert.equal(f.calls.filter(call => call.method === 'access.principals.revoke').at(-1).args.principalId, principalId);
  f.handlers.delete('access.principals.revoke'); f.button('Закрыть созданный доступ', f.dialog()).click(); await f.handle.flush(); await settle();
  assert.equal(f.dialog(), null); assert.equal(f.data.principals.find(item => item.id === 'different_principal').state, 'active');
  assert.equal(f.calls.filter(call => call.method === 'access.credentials.issue').length, 1);
});

test('account A to B to A remount discards late one-shot key and cannot resume the old form', async t => {
  const f = fixture(t); await settle(); const form = await f.openCreate(); fill(form, true);
  const held = deferred(); f.handlers.set('access.credentials.issue', () => held.promise);
  form.dispatch('submit'); await settle(); assert.equal(f.calls.filter(call => call.method === 'access.credentials.issue').length, 1);
  const old = f.handle, args = f.calls.find(call => call.method === 'access.credentials.issue').args;
  f.mount('owner_b'); await settle(); f.mount('owner_a'); await settle();
  held.resolve({ token: `soty_cap_${'q'.repeat(43)}`, credential: { id: 'synthetic', grantId: args.grantId, audience: args.audience, expiresAt: args.expiresAt } });
  await old.flush(); await settle();
  assert.equal(f.dialog(), null); assert.equal(f.document.body.querySelector('.sa-secret'), null);
  const count = f.writes().length; form.dispatch('submit'); await settle(); assert.equal(f.writes().length, count);
  assert.equal(f.handle.hasUnsavedChanges(), false);
});

test('child presentation uses actual parent and root IDs with the shared budget and no inferred live-chain authority', async t => {
  const child = principal('child'), parentId = 'grant_parent_' + 'p'.repeat(90), rootId = 'grant_root_' + 'r'.repeat(90);
  const childGrant = grant('grant_child', child.id, { parentGrantId: parentId, rootGrantId: rootId, depth: 1 });
  const f = fixture(t, { data: { principals: [child], grants: [childGrant] } }); await settle();
  f.target('principal-child').click(); await settle();
  const card = f.host.querySelector('.sa-grant'); assert.ok(card);
  assert.match(card.textContent, /Доступ помощника/u); assert.match(card.textContent, /Общий лимитОсталось 7 из 10 действий на всю цепочку/u);
  assert.match(card.textContent, /Выдача записана/u); assert.match(card.textContent, /Доступ зависит от ключа и всей цепочки/u);
  assert.ok(!card.textContent.includes('Действует')); assert.ok(!card.textContent.includes('Доступ активен'));
  const precise = card.querySelector('details'); assert.equal(precise.open, false);
  assert.ok(precise.textContent.includes(parentId)); assert.ok(precise.textContent.includes(rootId)); assert.ok(precise.textContent.includes('grant_child'));
  assert.match(precise.textContent, /Подключать новых помощников нельзя/u);
  assert.ok(f.host.textContent.includes(child.id));
  f.target('revoke-grant-grant_child').click(); await settle();
  assert.ok(f.dialog().textContent.includes('grant_child')); assert.ok(f.dialog().textContent.includes(child.id));
  assert.match(f.dialog().textContent, /Отзыв одного ключа не отключает/u);
  f.button('Отозвать доступ', f.dialog()).click(); await f.handle.flush(); await settle();
  assert.deepEqual(f.writes().map(call => [call.method, call.args.grantId]), [['access.grants.revoke', 'grant_child']]);
  assert.equal(f.host.querySelector('.sa-state').textContent, 'Отозван');
});

test('service audit remains distinct from a signed owner device and preserves exact principal IDs', async t => {
  const f = fixture(t, { data: { events: [
    { id: 'e1', kind: 'access.grants.derive', objectType: 'grant', objectId: 'child_grant', actorType: 'service', actorId: 'principal_exact_parent', createdAt: time },
    { id: 'e2', kind: 'access.grants.issue', objectType: 'grant', objectId: 'owner_grant', actorType: 'connect', actorId: 'device_exact_owner', createdAt: time },
  ] } }); await settle();
  f.button('История доступов').click(); await settle();
  const rows = f.host.querySelectorAll('.sa-event'); assert.equal(rows.length, 2);
  assert.match(rows[0].textContent, /Передал клиентprincipal_exact_parent/u); assert.ok(!rows[0].textContent.includes('Изменено с устройства'));
  assert.match(rows[1].textContent, /Изменено с устройстваdevice_exact_owner/u); assert.ok(!rows[1].textContent.includes('Передал клиент'));
  const event = f.button('История доступов').dispatch('keydown', { key: 'Home' }); await settle();
  assert.equal(event.defaultPrevented, true); assert.equal(f.document.activeElement, f.button('Клиенты'));
});

test('pagehide erases a displayed secret and reopening starts with delegation off without issuing another key', async t => {
  const f = fixture(t); await settle(); const form = await f.openCreate(); fill(form, true);
  form.dispatch('submit'); await f.handle.flush(); await settle(); const input = f.dialog().querySelector('.sa-secret');
  assert.ok(input.value.length > 0); f.windowEvents.get('pagehide')(); assert.equal(input.value.length, 0); assert.equal(f.dialog(), null);
  const writes = f.writes().length, next = await f.openCreate();
  assert.equal(next.querySelector('.sa-delegation-check').checked, false); assert.equal(f.writes().length, writes);
});
