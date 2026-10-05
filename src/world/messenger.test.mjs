import test from 'node:test';
import assert from 'node:assert/strict';
import { canGroupMessages, chatDayKey, chatDayLabel, chatListTime, chatPreview, shouldSendOnEnter } from './messenger.mjs';

const message = (createdAt, overrides = {}) => ({ createdAt, author: { profileId: 'alice' }, removed: false, replyTo: null, ...overrides });

test('calendar separators distinguish midnight, yesterday and previous years', () => {
  const now = new Date(2026, 0, 1, 0, 5);
  assert.equal(chatDayLabel(now, now), 'Сегодня');
  assert.equal(chatDayLabel(new Date(2025, 11, 31, 23, 58), now), 'Вчера');
  assert.equal(chatDayKey(new Date(2025, 11, 31)), '2025-12-31');
  assert.match(chatDayLabel(new Date(2024, 11, 30), now), /2024/);
  assert.equal(chatDayLabel('invalid', now), '');
  assert.equal(chatListTime(new Date(2025, 11, 31), now), '31.12.25');
});

test('message groups break at date, author, reply, removal and five minute boundaries', () => {
  const at = new Date(2026, 9, 5, 12).getTime(), first = message(at);
  assert.equal(canGroupMessages(first, message(at + 299_999)), true);
  assert.equal(canGroupMessages(first, message(at + 300_000)), false);
  assert.equal(canGroupMessages(first, message(at - 1)), false);
  assert.equal(canGroupMessages(first, message(at + 1, { replyTo: 'msg-other' })), false);
  assert.equal(canGroupMessages(first, message(at + 1, { author: { profileId: 'bob' } })), false);
  assert.equal(canGroupMessages(first, message(at + 1, { removed: true })), false);
  assert.equal(canGroupMessages(message(at, { removed: true }), message(at + 1)), false);
  assert.equal(canGroupMessages(undefined, first), false);
  assert.equal(canGroupMessages(message(new Date(2026, 9, 5, 23, 59).getTime()), message(new Date(2026, 9, 6, 0, 1).getTime())), false);
});

test('Enter sends only complete unmodified input; IME confirmation never sends a message', () => {
  const enter = { key: 'Enter', shiftKey: false, altKey: false, ctrlKey: false, metaKey: false, isComposing: false, keyCode: 13 };
  assert.equal(shouldSendOnEnter(enter), true);
  for (const modifier of ['shiftKey', 'altKey', 'ctrlKey', 'metaKey', 'isComposing']) assert.equal(shouldSendOnEnter({ ...enter, [modifier]: true }), false);
  assert.equal(shouldSendOnEnter({ ...enter, keyCode: 229 }), false);
  assert.equal(shouldSendOnEnter({ ...enter, key: 'Escape' }), false);
});

test('previews flatten whitespace and never expose the deleted message body', () => {
  assert.equal(chatPreview({ text: '  Привет\n\nмир\t 👋  ', removed: false }), 'Привет мир 👋');
  assert.equal(chatPreview({ text: 'private old body', removed: true }), 'Сообщение удалено');
  assert.equal(chatPreview(undefined), '');
});
