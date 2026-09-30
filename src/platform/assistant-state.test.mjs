import test from 'node:test';
import assert from 'node:assert/strict';
import { createAssistantState as createStore } from './assistant-state.mjs';
function createAssistantState(options) { let first = true; return createStore({ ...options, randomId: () => { if (first) { first = false; return 'conversation-one'; } return crypto.randomUUID(); } }); }
const input = { expectedAccountId: 'account-one', hostDeviceId: 'host-one', connectorId: 'connector-one', conversationId: 'conversation-one', text: 'Проверь проект', cwd: 'D:\\проект' };
function storage() { const data = new Map(); return { getItem: k => data.get(k) ?? null, setItem: (k, v) => data.set(k, v) }; }
test('a lost ACK survives reload and retries with the same immutable request', () => {
  const disk = storage(), first = createAssistantState({ accountId: 'account-one', storage: disk });
  first.saveDraft({ text: input.text, cwd: input.cwd });
  const pending = first.prepare(input);
  const reopened = createAssistantState({ accountId: 'account-one', storage: disk });
  assert.deepEqual(reopened.prepare(input), pending);
  assert.throws(() => reopened.prepare({ ...input, hostDeviceId: 'other-host' }), /pending_unconfirmed/);
  assert.throws(() => reopened.select({ conversationId: 'another-conversation' }), /pending_unconfirmed/);
  reopened.acknowledge(pending.requestId, { hostDeviceId: 'host-one', connectorId: 'connector-one', jobId: 'job-one' });
  assert.equal(reopened.read().pending, null); assert.equal(reopened.read().text, '');
  assert.equal(reopened.read().lastJob.jobId, 'job-one');
});
test('a late ACK cannot erase another tab draft or newer pending request; accounts stay separate', () => {
  const disk = storage(), first = createAssistantState({ accountId: 'account-one', storage: disk });
  first.saveDraft({ text: input.text }); const pending = first.prepare(input);
  const second = createAssistantState({ accountId: 'account-one', storage: disk });
  second.saveDraft({ text: 'Следующее сообщение' });
  first.acknowledge(pending.requestId, { hostDeviceId: 'host-one', connectorId: 'connector-one', jobId: 'job-one' });
  assert.equal(second.read().text, 'Следующее сообщение');
  const next = second.prepare({ ...input, text: 'Следующее сообщение' });
  assert.equal(first.acknowledge(pending.requestId, { hostDeviceId: 'host-one', connectorId: 'connector-one', jobId: 'job-one' }), false);
  assert.equal(second.read().pending.requestId, next.requestId);
  const other = createAssistantState({ accountId: 'account-two', storage: disk });
  assert.equal(other.read().pending, null); assert.equal(other.read().text, '');
  assert.throws(() => other.prepare(input), /invalid_account/);
});
test('storage failure preserves the local draft and blocks dispatch before durable request creation', () => {
  const disk = storage(); let fail = true;
  const saved = createAssistantState({ accountId: 'account-one', storage: { getItem: disk.getItem, setItem(k,v) { if (fail) throw new Error('quota'); disk.setItem(k,v); } } });
  saved.saveDraft({ text: input.text });
  assert.equal(saved.hasUnsavedChanges(), true); assert.throws(() => saved.prepare(input), /storage_unavailable/);
  const requestId = saved.read().pending.requestId;
  fail = false; saved.flush(); assert.equal(saved.hasUnsavedChanges(), false);
  assert.equal(saved.prepare(input).requestId, requestId);
  assert.equal(createAssistantState({ accountId: 'account-one', storage: disk }).read().text, input.text);
});
test('unreadable saved data cannot be overwritten with a fresh dispatch', () => {
  const disk = storage(); disk.setItem('soty.assistant.v1:account-one', '{broken');
  const state = createAssistantState({ accountId: 'account-one', storage: disk });
  state.saveDraft({ text: input.text });
  assert.throws(() => state.prepare(input), /storage_unavailable/);
  assert.equal(disk.getItem('soty.assistant.v1:account-one'), '{broken');
  assert.equal(state.hasUnsavedChanges(), true);
});
test('drafts remain attached to their conversation across history, new conversations and another active tab', () => {
  const disk = storage(), first = createAssistantState({ accountId: 'account-one', storage: disk });
  first.saveDraft({ text: 'Только для проекта A', cwd: '/project-a', deviceKey: 'host-a:connector-a' });
  first.select({ conversationId: 'conversation-b', cwd: '/project-b', deviceKey: 'host-b:connector-b' });
  assert.equal(first.read().text, '');
  first.saveDraft({ text: 'Только для проекта B' });
  const second = createAssistantState({ accountId: 'account-one', storage: disk });
  assert.equal(second.read().text, 'Только для проекта B');
  first.select({ conversationId: 'conversation-one', cwd: '/project-a', deviceKey: 'host-a:connector-a' });
  assert.equal(first.read().text, 'Только для проекта A');
  second.saveDraft({ text: 'Новая задача B' });
  assert.equal(first.read().text, 'Только для проекта A'); assert.equal(first.read().conversationId, 'conversation-one');
  first.saveDraft({ text: 'Новая задача A' });
  assert.equal(second.read().text, 'Новая задача B'); assert.equal(second.read().conversationId, 'conversation-b');
  assert.deepEqual(new Set(first.listDrafts().map(value => value.text)), new Set(['Новая задача A', 'Новая задача B']));
  first.select({ conversationId: 'conversation-new' }); assert.equal(first.read().text, '');
  for (let i = 0; i < 150; i++) first.select({ conversationId: `empty-conversation-${i}` });
  assert.equal(first.listDrafts().length, 2, 'empty visits cannot consume the durable draft limit');
});
