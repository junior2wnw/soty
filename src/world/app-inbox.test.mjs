import test from 'node:test';
import assert from 'node:assert/strict';
import { loadAppDiscussionInbox, markAppDiscussionRead, recentAppDiscussionTargets } from './app-inbox.mjs';
import { formatAppLaunchRoute } from './app-launch.mjs';
const appId = `app-${'a'.repeat(32)}`, entry = { appId, domainId: `dom_${'b'.repeat(32)}`, origin: 'https://work.example', path: '/my?q=a%2Bb' };
const row = () => ({ appId, name: 'Работа', entry, conversationId: `conv_${'c'.repeat(32)}`, unreadCount: 2,
  lastMessage: { id: `msg_${'d'.repeat(32)}`, author: { accountId: 'B', label: 'Участник' }, preview: 'Сообщение', createdAt: 1800000000000 } });
const response = discussions => ({ schema: 'soty.app-discussion.inbox.v1', discussions, checkedAt: 1800000000001 });

test('recent discussion discovery retains exact named paths and ignores malformed or generic history', () => {
  const target = { appId, domainId: entry.domainId, path: entry.path };
  const route = formatAppLaunchRoute(target, undefined, { panel: 'discussion', conversationId: row().conversationId });
  assert.deepEqual(recentAppDiscussionTargets([{ route }, { route }, { route: 'https://foreign.invalid' }, { route: 'messages' }, { route: 'app/' + appId }]), [target]);
});

test('inbox uses exact permitted locations without launch/admission and retains server unread', async () => {
  const calls = [], api = { request: async (operation, args) => { calls.push([operation, args]); return response([row()]); } };
  const value = await loadAppDiscussionInbox({ api, accountId: 'A', entries: [entry, entry], isCurrent: () => true });
  assert.equal(calls.length, 1); assert.equal(calls[0][0], 'apps.discussion.inbox');
  assert.deepEqual(calls[0][1], { expectedAccountId: 'A', entries: [{ appId, domainId: entry.domainId, path: entry.path }] });
  assert.equal(value.discussions[0].unreadCount, 2);
});

test('empty or unsupported app inbox does not manufacture known unread', async () => {
  const unsupported = await loadAppDiscussionInbox({ api: { request: async () => { throw { code: 'unknown_operation' }; } }, accountId: 'A', entries: [entry], isCurrent: () => true });
  assert.equal(unsupported.supported, false); assert.equal(unsupported.checkedAt, null);
  const empty = await loadAppDiscussionInbox({ api: { request: async () => response([]) }, accountId: 'A', entries: [entry], isCurrent: () => true });
  assert.deepEqual(empty.discussions, []);
});

test('optional read metadata failure keeps authorised content and an explicitly unknown unread count', async () => {
  const value = await loadAppDiscussionInbox({ api: { request: async () => ({ ...response([{ ...row(), unreadCount: null }]), readState: 'unavailable' }) },
    accountId: 'A', entries: [entry], isCurrent: () => true });
  assert.equal(value.discussions[0].lastMessage.preview, 'Сообщение');
  assert.equal(value.discussions[0].unreadCount, null); assert.equal(value.readState, 'unavailable');
});

test('profile changes discard responses; another entry, duplicate app and invalid unread are rejected', async () => {
  let current = true;
  const stale = await loadAppDiscussionInbox({ api: { request: async () => { current = false; return response([row()]); } }, accountId: 'A', entries: [entry], isCurrent: () => current });
  assert.equal(stale, null);
  for (const rows of [[{ ...row(), entry: { ...entry, path: '/other' } }], [row(), row()], [{ ...row(), unreadCount: -1 }]]) {
    await assert.rejects(loadAppDiscussionInbox({ api: { request: async () => response(rows) }, accountId: 'A', entries: [entry], isCurrent: () => true }), error => error.code === 'app_discussion_invalid_inbox');
  }
});

test('read accepts only a receipt for the displayed conversation and message of the current account', async () => {
  const conversationId = row().conversationId, messageId = row().lastMessage.id;
  const args = { accountId: 'A', entry, conversationId, messageId, isCurrent: () => true };
  let input;
  const receipt = { schema: 'soty.app-discussion.read.v1', appId, conversationId, messageId, unreadCount: 0, readAt: 1800000000002 };
  const result = await markAppDiscussionRead({ ...args, api: { request: async (operation, value) => { input = [operation, value]; return receipt; } } });
  assert.equal(input[0], 'apps.discussion.read'); assert.equal(input[1].expectedAccountId, 'A'); assert.equal(input[1].messageId, messageId);
  assert.equal(result.unreadCount, 0);
  await assert.rejects(markAppDiscussionRead({ ...args, api: { request: async () => ({ ...receipt, conversationId: `conv_${'e'.repeat(32)}` }) } }), error => error.code === 'app_discussion_invalid_inbox');
  let current = true;
  assert.equal(await markAppDiscussionRead({ ...args, isCurrent: () => current, api: { request: async () => { current = false; return receipt; } } }), null);
});
