import test from 'node:test';
import assert from 'node:assert/strict';
import { createAssistantState } from './assistant-state.mjs';

const accountId = 'acceptance-account';
const deviceA = { hostDeviceId: 'host-alpha', connectorId: 'connector-alpha' };
const deviceB = { hostDeviceId: 'host-beta', connectorId: 'connector-beta' };
const deviceKey = value => `${value.hostDeviceId}:${value.connectorId}`;
function disk() {
  const records = new Map();
  return { records, getItem: key => records.get(key) ?? null, setItem: (key, value) => records.set(key, value) };
}
function store(storage, prefix, extra = {}) {
  let sequence = 0;
  return createAssistantState({ accountId, storage, randomId: () => `${prefix}-${++sequence}`, ...extra });
}
function input(state, overrides = {}) {
  return { expectedAccountId: accountId, ...deviceA, conversationId: state.read().conversationId,
    text: 'Проверь только этот проект', cwd: '/projects/alpha', ...overrides };
}

test('restoring an older list item cannot pair fresh text with an obsolete device or folder', () => {
  const storage = disk(), first = store(storage, 'first');
  first.saveDraft({ text: 'Черновик A', cwd: '/projects/alpha', deviceKey: deviceKey(deviceA) });
  const olderListItem = first.listDrafts()[0];
  first.select({ conversationId: 'another-conversation', cwd: '/other', deviceKey: deviceKey(deviceA) });
  const second = store(storage, 'second');
  second.select(olderListItem);
  second.saveDraft({ text: 'Задача для нового устройства', cwd: '/projects/beta', deviceKey: deviceKey(deviceB) });

  const restored = first.restore(olderListItem.conversationId);
  assert.equal(restored.text, 'Задача для нового устройства');
  assert.equal(restored.deviceKey, deviceKey(deviceB), 'restoring a saved draft reads its current binding');
  assert.equal(restored.cwd, '/projects/beta');
});

test('remount and reload return to this tab conversation after a second tab changes the shared index', () => {
  const storage = disk(), firstTab = disk(), secondTab = disk();
  const first = store(storage, 'first', { tabStorage: firstTab });
  first.saveDraft({ text: 'Разговор вкладки A', cwd: '/projects/alpha', deviceKey: deviceKey(deviceA) });
  const firstId = first.read().conversationId;
  const second = store(storage, 'second', { tabStorage: secondTab });
  second.select({ conversationId: 'tab-b-conversation', cwd: '/projects/beta', deviceKey: deviceKey(deviceB) });
  second.saveDraft({ text: 'Разговор вкладки B' });
  const remountedFirst = store(storage, 'remounted-first', { tabStorage: firstTab });
  assert.equal(remountedFirst.read().conversationId, firstId);
  assert.equal(remountedFirst.read().text, 'Разговор вкладки A');
  assert.equal(remountedFirst.read().deviceKey, deviceKey(deviceA));
  remountedFirst.saveDraft({ text: 'Продолжение A' });
  const reloadedSecond = store(storage, 'reloaded-second', { tabStorage: secondTab });
  assert.equal(reloadedSecond.read().conversationId, 'tab-b-conversation');
  assert.equal(reloadedSecond.read().text, 'Разговор вкладки B');
});

test('an unconfirmed request remains discoverable even when its visible draft is empty', () => {
  const storage = disk(), first = store(storage, 'first'), other = store(storage, 'other');
  const request = first.prepare(input(first));
  other.saveDraft({ text: 'Отдельный разговор' });
  const pendingConversation = other.listDrafts().find(value => value.conversationId === request.payload.conversationId);
  assert.ok(pendingConversation, 'a pending request must have a recovery entry outside the active conversation');
  assert.match(pendingConversation.text, /Проверь только этот проект/);
});

test('only the matching request and exact execution target can acknowledge the pending operation', () => {
  const state = store(disk(), 'receipt');
  state.saveDraft({ text: 'Проверь только этот проект' });
  const pending = state.prepare(input(state));
  assert.equal(state.acknowledge('unrelated-request', { ...deviceA, jobId: 'job-alpha' }), false);
  assert.throws(() => state.acknowledge(pending.requestId, { ...deviceB, jobId: 'job-beta' }), /invalid_assistant_receipt/);
  assert.deepEqual(state.read().pending, pending);
  assert.equal(state.acknowledge(pending.requestId, { ...deviceA, jobId: 'job-alpha' }), true);
  assert.equal(state.read().pending, null);
});

test('copied state and pending payloads cannot mutate the stored retry target', () => {
  const storage = disk(), state = store(storage, 'copy');
  state.saveDraft({ text: 'Проверь только этот проект' });
  const value = input(state), pending = state.prepare(value), originalRequestId = pending.requestId;
  pending.payload.hostDeviceId = deviceB.hostDeviceId;
  const projected = state.read();
  projected.pending.payload.cwd = '/another-project';
  projected.text = 'Подменённый текст';
  const restored = store(storage, 'reopened');
  assert.equal(restored.read().text, value.text);
  assert.deepEqual(restored.prepare(value), { requestId: originalRequestId, payload: value });
});

test('a failed index write blocks dispatch and later recovers the same durable request', () => {
  const storage = disk();
  let rejectIndex = false;
  const guarded = { getItem: storage.getItem, setItem(key, value) {
    if (rejectIndex && key === `soty.assistant.v2:${accountId}`) throw new Error('quota');
    storage.setItem(key, value);
  } };
  const state = store(guarded, 'index');
  state.saveDraft({ text: 'Проверь только этот проект' });
  const value = input(state);
  rejectIndex = true;
  assert.throws(() => state.prepare(value), /assistant_storage_unavailable/);
  assert.equal(state.hasUnsavedChanges(), true);
  const requestId = state.read().pending.requestId;
  assert.equal(JSON.parse(storage.getItem(state.key)).pending.requestId, requestId);
  rejectIndex = false;
  state.flush();
  assert.equal(state.prepare(value).requestId, requestId);
  assert.equal(store(storage, 'reloaded').prepare(value).requestId, requestId);
});

test('foreign-account payloads and corrupted stored request ownership cannot be dispatched', () => {
  const storage = disk(), state = store(storage, 'ownership');
  state.saveDraft({ text: 'Проверь только этот проект' });
  assert.throws(() => state.prepare(input(state, { expectedAccountId: 'foreign-account' })), /invalid_account/);
  assert.equal(state.read().pending, null);
  state.prepare(input(state));
  const corrupted = JSON.parse(storage.getItem(state.key));
  corrupted.pending.payload.expectedAccountId = 'foreign-account';
  const raw = JSON.stringify(corrupted); storage.setItem(state.key, raw);
  const reopened = store(storage, 'reopened');
  assert.throws(() => reopened.prepare(input(reopened)), /assistant_storage_unavailable/);
  assert.equal(storage.getItem(state.key), raw, 'unreadable evidence is not replaced with a new request');
});

test('late rejection and acknowledgement never remove a newer request or later draft text', () => {
  const storage = disk(), first = store(storage, 'late');
  first.saveDraft({ text: 'Первая задача' });
  const old = first.prepare(input(first, { text: 'Первая задача' }));
  assert.equal(first.forgetRejected(old.requestId), true);
  first.saveDraft({ text: 'Вторая задача' });
  const next = first.prepare(input(first, { text: 'Вторая задача' }));
  const anotherTab = store(storage, 'another-tab');
  anotherTab.saveDraft({ text: 'Третий, ещё не отправленный текст' });
  assert.equal(first.forgetRejected(old.requestId), false);
  assert.equal(first.acknowledge(old.requestId, { ...deviceA, jobId: 'old-job' }), false);
  assert.equal(first.read().pending.requestId, next.requestId);
  first.acknowledge(next.requestId, { ...deviceA, jobId: 'current-job' });
  assert.equal(anotherTab.read().text, 'Третий, ещё не отправленный текст');
  assert.equal(anotherTab.read().pending, null);
});
