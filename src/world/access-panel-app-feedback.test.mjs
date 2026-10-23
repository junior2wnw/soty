import test from 'node:test';
import assert from 'node:assert/strict';
import { fixture, grant, audience, settle, deferred } from './test-support/access-panel-delegation.mjs';

const appId = 'app-' + 'a'.repeat(32), installationId = 'fbi_' + 'b'.repeat(32);
const target = { accountId: 'owner_a', appId, name: 'Моё приложение' };
const ready = { notesCreateEnabled: false, audience, appFeedbackEnabled: true, mcpAudience: audience + '/mcp' };
const context = () => ({ readiness: 'ready', context: { appId, installationId, canManage: true, ticketVisibility: 'reporter-and-support' } });
const calls = (f, method) => f.calls.filter(call => call.method === method);
function feedbackFixture(t, options = {}) {
  const f = fixture(t, { availability: async () => ready, panelOptions: { appFeedback: target }, ...options,
    handlers: { 'apps.feedback.context': context, ...options.handlers } });
  f.handlers.set('access.grants.issue', args => {
    const item = grant('feedback_grant', args.principalId, { ...args, accountId: args.expectedAccountId, rootGrantId: 'feedback_grant',
      budget: { ...args.budget, reserved: 0, spent: 0, remaining: args.budget.limit, uncertain: 0 } });
    delete item.expectedAccountId; f.data.grants.push(item); return { grant: structuredClone(item) };
  });
  return f;
}
function form(f) { const value = f.dialog()?.querySelector('form'); assert.ok(value); return value; }
function fill(f) { const value = form(f), name = value.querySelectorAll('input').find(node => node.type === 'text'); name.value = 'Мой внешний агент'; name.dispatch('input'); return value; }
async function submit(f) { fill(f).dispatch('submit'); await f.handle.flush(); await settle(); }

test('selected owner app opens the existing consent; default read works with Notes off and delivers exact MCP metadata', async t => {
  const f = feedbackFixture(t); await settle(); const value = form(f);
  assert.equal(value.querySelector('.sa-delegation-check'), null);
  assert.equal(value.querySelector('.sa-feedback-mode').value, 'read');
  assert.equal(value.querySelectorAll('input').find(node => node.type === 'text').value, 'Агент · ' + target.name);
  assert.equal(value.querySelectorAll('input').find(node => node.type === 'number').value, '100');
  assert.match(value.textContent, /Только обращения приложения/u); assert.ok(value.textContent.includes(appId));
  assert.equal(f.host.querySelectorAll('button').some(node => node.textContent === 'Создать доступ по ключу'), false, 'selected app has one clear create action');
  await submit(f);
  const issued = calls(f, 'access.grants.issue')[0].args;
  assert.deepEqual(issued.capabilities, ['list', 'get'].map(operation => ({ capabilityId: 'apps.feedback.' + operation, version: 1 })));
  assert.deepEqual(issued.resources, ['apps.feedback', 'apps.feedback:' + appId]);
  assert.deepEqual(issued.effects, []); assert.deepEqual(issued.recipients, ['soty:app-feedback']);
  assert.equal(issued.allowDelegation, false); assert.equal(issued.maxDepth, 0);
  assert.deepEqual(issued.budget, { unit: 'invocations', limit: 100 });
  assert.equal(calls(f, 'access.credentials.issue')[0].args.audience, audience + '/mcp');
  assert.equal(calls(f, 'apps.feedback.context').length, 3, 'native owner preparation is separate from agent capabilities');
  assert.ok(f.calls.filter(call => call.method !== 'apps.feedback.context').every(call => call.args.expectedAccountId === 'owner_a'));
  assert.ok(calls(f, 'apps.feedback.context').every(call => JSON.stringify(call.args) === JSON.stringify({ appId })));
  const secret = f.dialog().querySelector('.sa-secret'); assert.equal(secret.type, 'password');
  assert.ok(f.dialog().textContent.includes(installationId)); assert.ok(!f.dialog().textContent.includes(secret.value));
  f.button('Скопировать параметры подключения', f.dialog()).click(); await settle();
  assert.deepEqual(JSON.parse(f.copiedValues[0]), { endpoint: audience + '/mcp', authorization: `Bearer soty_cap_${'q'.repeat(43)}`, appId, installationId });
  f.button('Готово, закрыть', f.dialog()).click(); assert.equal(secret.value, ''); assert.equal(f.handle.hasUnsavedChanges(), false);
  assert.equal(f.document.activeElement, f.button('Подключить агента к обращениям', f.host));
});

test('cancelling initial consent creates no grant and returns keyboard focus to its exact app action', async t => {
  const f = feedbackFixture(t); await settle();
  f.button('Закрыть', f.dialog()).click();
  assert.equal(f.dialog(), null); assert.equal(calls(f, 'access.principals.create').length, 0);
  assert.equal(f.document.activeElement, f.button('Подключить агента к обращениям', f.host));
});

test('triage consent is captured before awaits and does not grant context, submit, accept or delegation', async t => {
  const f = feedbackFixture(t); await settle(); const value = fill(f), mode = value.querySelector('.sa-feedback-mode');
  mode.value = 'triage'; mode.dispatch('change'); const held = deferred(); f.controls.availability = () => held.promise;
  value.dispatch('submit'); assert.equal(mode.disabled, true); mode.value = 'read'; mode.dispatch('change'); value.dispatch('submit');
  held.resolve(ready); await f.handle.flush(); await settle();
  assert.equal(calls(f, 'access.grants.issue').length, 1);
  const args = calls(f, 'access.grants.issue')[0].args;
  assert.deepEqual(args.capabilities.map(item => item.capabilityId), ['list', 'get', 'reply', 'status'].map(operation => 'apps.feedback.' + operation));
  assert.deepEqual(args.effects, ['update']); assert.equal(args.allowDelegation, false);
});

test('old backend and noncanonical MCP audiences refuse before context or issuance, independently of Notes', async t => {
  for (const value of [
    { notesCreateEnabled: true, audience },
    { ...ready, appFeedbackEnabled: false }, { ...ready, mcpAudience: audience },
    { ...ready, mcpAudience: 'https://other.example/mcp' }, { ...ready, mcpAudience: audience + '/mcp?token=synthetic' },
    { ...ready, audience: audience + '/prefix', mcpAudience: audience + '/prefix/mcp' },
  ]) await t.test('unavailable selected app', async t => {
    const f = feedbackFixture(t, { availability: async () => value }); await settle();
    assert.match(f.dialog().textContent, /Сервер пока не поддерживает/u);
    assert.equal(f.button('Подключить агента к обращениям', f.host).disabled, true);
    assert.equal(calls(f, 'apps.feedback.context').length + calls(f, 'access.principals.create').length, 0);
  });
});

test('foreign-account and malformed placement presets never open or prepare a queue', async t => {
  for (const selected of [{ ...target, accountId: 'owner_b' }, { ...target, appId: 'fp_' + 'a'.repeat(32) }]) {
    const f = feedbackFixture(t, { panelOptions: { appFeedback: selected } }); await settle();
    assert.equal(f.dialog(), null); assert.equal(calls(f, 'apps.feedback.context').length, 0);
  }
});

test('context requires current owner, exact app and installation, and private support visibility', async t => {
  for (const change of [value => value.context.canManage = false, value => value.context.appId = 'app-' + 'c'.repeat(32),
    value => value.context.installationId = 'fp_' + 'd'.repeat(32), value => value.context.ticketVisibility = 'public', value => value.readiness = 'pending']) {
    const f = feedbackFixture(t, { handlers: { 'apps.feedback.context': () => { const value = context(); change(value); return value; } } }); await settle();
    assert.match(f.dialog().textContent, /Не удалось подтвердить вашу очередь/u);
    assert.equal(calls(f, 'access.principals.create').length, 0);
  }
});

test('lost owner or replaced queue at consent submit refuses before principal creation', async t => {
  const f = feedbackFixture(t); await settle();
  f.handlers.set('apps.feedback.context', () => ({ ...context(), context: { ...context().context, installationId: 'fbi_' + 'c'.repeat(32) } }));
  await submit(f); assert.equal(calls(f, 'access.principals.create').length, 0); assert.equal(calls(f, 'access.credentials.issue').length, 0);
});

test('authority is rechecked after grant; changed queue cannot receive a credential', async t => {
  const f = feedbackFixture(t); await settle(); let preparations = 0;
  f.handlers.set('apps.feedback.context', () => { const value = context(); if (++preparations === 2) value.context.canManage = false; return value; });
  await submit(f); assert.equal(calls(f, 'access.grants.issue').length, 1); assert.equal(calls(f, 'access.credentials.issue').length, 0);
  f.button('Закрыть созданный доступ', f.dialog()).click(); await f.handle.flush(); await settle();
  assert.equal(calls(f, 'access.principals.revoke')[0].args.principalId, f.data.principals[0].id); assert.equal(f.dialog(), null);
});

test('unexpected returned grant scopes and authority never lead to a credential', async t => {
  const changes = [
    value => value.capabilities.push({ capabilityId: 'apps.feedback.context', version: 1 }),
    value => value.capabilities[0].version = 2, value => value.capabilities[0].version = '1', value => value.capabilities[0].allApps = true,
    value => value.capabilities.push(value.capabilities[0]),
    value => value.resources.pop(), value => value.resources.push('apps.feedback:app-' + 'c'.repeat(32)),
    value => value.effects.push('update'), value => value.recipients.push('soty:notes'),
    value => value.allowDelegation = true, value => value.maxDepth = 1,
    value => value.accountId = 'owner_b', value => value.clientId = 'different_client',
    value => value.rootGrantId = 'different_root', value => value.parentGrantId = 'parent', value => value.depth = 1,
    value => value.expiresAt += 1, value => value.revokedAt = 1, value => value.budget.limit += 1,
  ];
  for (const change of changes) await t.test('reject unconfirmed grant response', async t => {
    const f = feedbackFixture(t); await settle(); const issue = f.handlers.get('access.grants.issue');
    f.handlers.set('access.grants.issue', args => { const result = issue(args); change(result.grant); return result; });
    await submit(f); assert.equal(calls(f, 'access.grants.issue').length, 1); assert.equal(calls(f, 'access.credentials.issue').length, 0);
    assert.equal(f.dialog().querySelector('.sa-secret'), null); assert.match(f.dialog().textContent, /Настройка не подтверждена/u);
  });
});

test('uncertain key response stays one-shot with existing revoke/history recovery and no secret', async t => {
  const f = feedbackFixture(t); await settle(); const value = fill(f);
  f.handlers.set('access.credentials.issue', () => { throw new Error('synthetic_response_lost'); });
  value.dispatch('submit'); await f.handle.flush(); await settle();
  assert.match(f.dialog().textContent, /Ключ мог быть выдан/u); assert.equal(f.dialog().querySelector('.sa-secret'), null);
  value.dispatch('submit'); await f.handle.flush(); assert.equal(calls(f, 'access.credentials.issue').length, 1);
  f.button('Закрыть созданный доступ', f.dialog()).click(); await f.handle.flush(); await settle();
  assert.equal(f.data.principals[0].state, 'revoked'); assert.equal(f.dialog(), null);
});

test('account switch discards a late key and invalidates stale consent without carrying target to another account', async t => {
  const f = feedbackFixture(t); await settle(); const value = fill(f), held = deferred();
  f.handlers.set('access.credentials.issue', () => held.promise); value.dispatch('submit'); await settle();
  const old = f.handle, args = calls(f, 'access.credentials.issue')[0].args;
  f.mount('owner_b'); await settle();
  held.resolve({ token: `soty_cap_${'q'.repeat(43)}`, credential: { grantId: args.grantId, audience: args.audience, expiresAt: args.expiresAt } });
  await old.flush(); await settle(); assert.equal(f.dialog(), null); assert.equal(f.document.body.querySelector('.sa-secret'), null);
  const count = f.calls.length; value.dispatch('submit'); await settle(); assert.equal(f.calls.length, count);
});

test('pagehide clears masked key and suppresses queued configuration copy', async t => {
  const f = feedbackFixture(t); await settle(); await submit(f); const secret = f.dialog().querySelector('.sa-secret');
  f.button('Скопировать параметры подключения', f.dialog()).click(); f.windowEvents.get('pagehide')(); await settle();
  assert.equal(secret.value, ''); assert.equal(f.copied, 0); assert.equal(f.dialog(), null);
});
