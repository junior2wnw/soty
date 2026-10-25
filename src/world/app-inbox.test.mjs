import test from 'node:test';
import assert from 'node:assert/strict';
import { loadAppDiscussionInbox, markAppDiscussionRead } from './app-inbox.mjs';
const appId = `app-${'a'.repeat(32)}`, entry = { appId, domainId: `dom_${'b'.repeat(32)}`, origin: 'https://work.example', path: '/my?q=a%2Bb' };
const row = () => ({ appId, source: 'requested', name: 'Работа', entry, conversationId: `conv_${'c'.repeat(32)}`, unreadCount: 2,
  lastMessage: { id: `msg_${'d'.repeat(32)}`, author: { accountId: 'B', label: 'Участник' }, preview: 'Сообщение', createdAt: 1800000000000 } });
const response = discussions => ({ schema: 'soty.app-discussion.inbox.v1', discussions, readState: 'synced', nextCursor: null, resetRequired: false, checkedAt: 1800000000001 });

test('inbox uses exact permitted locations without launch/admission and retains server unread', async () => {
  const calls = [], api = { request: async (operation, args) => { calls.push([operation, args]); return response([row()]); } };
  const value = await loadAppDiscussionInbox({ api, accountId: 'A', entries: [entry, entry], isCurrent: () => true });
  assert.equal(calls.length, 1); assert.equal(calls[0][0], 'apps.discussion.inbox');
  assert.deepEqual(calls[0][1], { expectedAccountId: 'A', entries: [{ appId, domainId: entry.domainId, path: entry.path }], includeKnown: true });
  assert.equal(value.discussions[0].unreadCount, 2);
});

test('a cold inbox discovers other-device participation with one server call and no client catalogue', async () => {
  const calls = [], api = { request: async (op, args) => { calls.push([op, args]); return response([{ ...row(), source: 'known' }]); } };
  const result = await loadAppDiscussionInbox({ api, accountId: 'B', entries: [], isCurrent: () => true });
  assert.deepEqual(calls, [['apps.discussion.inbox', { expectedAccountId: 'B', entries: [], includeKnown: true }]]);
  assert.equal(result.discussions[0].entry.path, entry.path); assert.equal(result.knownPages, 1);
});

test('a fresh known fallback may use a new location without modifying the saved request', async () => {
  const original = structuredClone(entry), current = { ...entry, domainId: `dom_${'e'.repeat(32)}`, origin: 'https://new.example', path: '/new' };
  const result = await loadAppDiscussionInbox({ api: { request: async () => response([{ ...row(), source: 'known', entry: current }]) },
    accountId: 'A', entries: [entry], isCurrent: () => true });
  assert.deepEqual(result.discussions[0].entry, current); assert.deepEqual(entry, original);
});

test('cursor pages continue through an empty intermediate page and preserve exact requested priority', async () => {
  const calls = [], current = { ...entry, path: '/current' };
  const pages = [{ ...response([row()]), nextCursor: 'first' }, { ...response([]), nextCursor: 'second' },
    response([{ ...row(), source: 'known', entry: current }])];
  const result = await loadAppDiscussionInbox({ api: { request: async (op, args) => { calls.push(args); return pages.shift(); } },
    accountId: 'A', entries: [entry], maxKnownPages: 3, isCurrent: () => true });
  assert.equal(calls.length, 3); assert.equal(calls[1].cursor, 'first'); assert.deepEqual(calls[1].entries, []);
  assert.equal(calls[2].cursor, 'second'); assert.equal(result.discussions.length, 1);
  assert.equal(result.discussions[0].entry.path, entry.path); assert.equal(result.nextCursor, null);
});

test('empty discovery pages continue within a bound and expose their remaining cursor', async () => {
  let calls = 0;
  const result = await loadAppDiscussionInbox({ api: { request: async () => ({ ...response([]), nextCursor: `page-${++calls}` }) },
    accountId: 'A', entries: [], isCurrent: () => true });
  assert.equal(calls, 3); assert.equal(result.nextCursor, 'page-3'); assert.equal(result.knownPages, 3);
  const following = await loadAppDiscussionInbox({ api: { request: async (op, args) => {
    assert.equal(args.cursor, 'page-3'); assert.deepEqual(args.entries, []); return response([{ ...row(), source: 'known' }]);
  } }, accountId: 'A', cursor: result.nextCursor, isCurrent: () => true });
  assert.equal(following.discussions.length, 1); assert.equal(following.nextCursor, null);
});

test('additional exact batches disable server discovery and retain the first page continuation', async () => {
  const entries = Array.from({ length: 51 }, (_, i) => ({ ...entry, appId: `app-${i.toString(16).padStart(32, '0')}` }));
  const calls = [];
  const result = await loadAppDiscussionInbox({ api: { request: async (op, args) => {
    calls.push(args); return { ...response(args.entries.map(value => ({ ...row(), appId: value.appId, entry: { ...entry, appId: value.appId } }))),
      nextCursor: args.includeKnown ? 'continuation' : null };
  } }, accountId: 'A', entries, isCurrent: () => true });
  assert.equal(calls.length, 2); assert.equal(calls[0].entries.length, 50); assert.equal(calls[1].entries.length, 1);
  assert.equal(calls[1].includeKnown, false); assert.equal(calls[1].cursor, undefined);
  assert.equal(result.discussions.length, 51); assert.equal(result.nextCursor, 'continuation');
});

test('stale cursor returns a reset request without merging a partial list', async () => {
  const result = await loadAppDiscussionInbox({ api: { request: async () => ({ ...response([]), resetRequired: true, readState: 'unavailable' }) },
    accountId: 'A', cursor: 'expired', isCurrent: () => true });
  assert.equal(result.resetRequired, true); assert.deepEqual(result.discussions, []);
});

test('malformed or repeated cursors, unrequested locations and unknown source cannot extend discovery', async () => {
  for (const value of [response([{ ...row(), source: 'invented' }]), response([row()]), { ...response([]), nextCursor: 'x'.repeat(2049) },
    { ...response([]), nextCursor: 'bad\n' }, { ...response([]), resetRequired: 'no' }]) {
    await assert.rejects(loadAppDiscussionInbox({ api: { request: async () => value }, accountId: 'A', entries: [], isCurrent: () => true }));
  }
  let calls = 0;
  await assert.rejects(loadAppDiscussionInbox({ api: { request: async () => { calls++; return { ...response([]), nextCursor: 'loop' }; } },
    accountId: 'A', entries: [], isCurrent: () => true }));
  assert.equal(calls, 2);
});

test('profile switches between discovery pages discard all preceding account data', async () => {
  let current = true, calls = 0;
  const result = await loadAppDiscussionInbox({ api: { request: async () => { if (++calls === 2) current = false;
    return { ...response([{ ...row(), source: 'known' }]), nextCursor: 'next' + calls };
  } }, accountId: 'A', entries: [], maxKnownPages: 2, isCurrent: () => current });
  assert.equal(result, null); assert.equal(calls, 2);
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
