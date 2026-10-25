import { normalizeAppEntry } from './app-saved-state.mjs';

const CONVERSATION = /^conv_[a-f0-9]{32}$/u, MESSAGE = /^msg_[a-f0-9]{32}$/u;
const integer = value => Number.isSafeInteger(value) && value >= 0;
const text = (value, max) => typeof value === 'string' && value.length <= max && !/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/u.test(value);
const fail = () => { throw Object.assign(new Error('Invalid discussion inbox'), { code: 'app_discussion_invalid_inbox' }); };
export const appDiscussionInboxUnsupported = error => ['unknown_operation', 'unsupported_operation', 'capability_not_supported', 'operation_unavailable'].includes(error?.code);

/** Inputs are locations, not authority. Root independently checks every entry. */
export async function loadAppDiscussionInbox({ api, accountId, entries = [], cursor = null, maxKnownPages = 1, isCurrent }) {
  const validCursor = value => typeof value === 'string' && value.length > 0 && value.length <= 2048 && !/[\u0000-\u001f\u007f]/u.test(value);
  if (cursor !== null && !validCursor(cursor) || !Number.isSafeInteger(maxKnownPages) || maxKnownPages < 1 || cursor && entries.length) fail();
  const targets = new Map();
  for (const value of entries) { const entry = normalizeAppEntry(value); if (!targets.has(entry.appId)) targets.set(entry.appId, entry); }
  const source = [...targets.values()], discussions = new Map(), cursors = new Set(cursor ? [cursor] : []);
  let checkedAt = 0, readState = 'synced', nextCursor = cursor, knownPages = 0, offset = 0;
  while (true) {
    if (!isCurrent()) return null;
    const includeKnown = offset === 0, batch = includeKnown && nextCursor ? [] : source.slice(offset, offset + 50);
    const expected = new Map(batch.map(entry => [entry.appId, entry]));
    let response;
    try { response = await api.request('apps.discussion.inbox', { expectedAccountId: accountId,
      entries: batch.map(({ appId, domainId, path }) => ({ appId, domainId, path })), includeKnown,
      ...(includeKnown && nextCursor ? { cursor: nextCursor } : {}) }); }
    catch (error) { if (!isCurrent()) return null; if (appDiscussionInboxUnsupported(error)) return { supported: false, discussions: [], checkedAt: null, nextCursor: null, knownPages: 0, resetRequired: false }; throw error; }
    if (!isCurrent()) return null;
    if (response?.schema !== 'soty.app-discussion.inbox.v1' || !Array.isArray(response.discussions)
      || response.discussions.length > 50 || !integer(response.checkedAt) || !['synced', 'unavailable'].includes(response.readState)
      || typeof response.resetRequired !== 'boolean' || !(response.nextCursor === null || validCursor(response.nextCursor))) fail();
    if (response.resetRequired) {
      if (response.discussions.length || response.nextCursor !== null) fail();
      return { supported: true, discussions: [], checkedAt: response.checkedAt, readState: response.readState, nextCursor: null, knownPages, resetRequired: true };
    }
    if (response.nextCursor && (!includeKnown || cursors.has(response.nextCursor))) fail();
    if (response.nextCursor) cursors.add(response.nextCursor);
    const seen = new Set();
    for (const row of response.discussions) {
      const entry = normalizeAppEntry(row.entry), requested = expected.get(row.appId);
      if (seen.has(row.appId) || entry.appId !== row.appId || !text(row.name, 160)
        || !(row.source === 'known' && includeKnown || row.source === 'requested' && requested && entry.domainId === requested.domainId
          && entry.path === requested.path && entry.origin === requested.origin)
        || !(integer(row.unreadCount) || row.unreadCount === null && response.readState === 'unavailable')
        || !(row.conversationId === null || CONVERSATION.test(row.conversationId))) fail();
      seen.add(row.appId);
      let lastMessage = null;
      if (row.lastMessage !== null) {
        const message = row.lastMessage;
        if (!message || !MESSAGE.test(message.id) || !integer(message.createdAt) || !text(message.preview, 1000)
          || !text(message.author?.accountId, 180) || !text(message.author?.label, 160) || !row.conversationId) fail();
        lastMessage = { id: message.id, author: { accountId: message.author.accountId, label: message.author.label }, preview: message.preview, createdAt: message.createdAt };
      }
      if (!row.conversationId && (row.unreadCount || lastMessage)) fail();
      // A server-known location is only a discussion target. It never rewrites a saved entry.
      if (!discussions.has(row.appId) || row.source === 'requested' || discussions.get(row.appId).source !== 'requested')
        discussions.set(row.appId, { appId: row.appId, name: row.name, entry, source: row.source, conversationId: row.conversationId, lastMessage, unreadCount: row.unreadCount });
    }
    checkedAt = Math.max(checkedAt, response.checkedAt);
    if (response.readState === 'unavailable') readState = 'unavailable';
    if (includeKnown) {
      knownPages++; nextCursor = response.nextCursor;
      // Empty or inaccessible intermediate pages are not EOF. Keep work bounded
      // and expose any remaining cursor to the explicit continuation control.
      if (nextCursor && (knownPages < maxKnownPages || ![...discussions.values()].some(row => row.conversationId) && knownPages < 3)) continue;
      offset = 50;
    } else offset += 50;
    if (offset >= source.length) break;
  }
  return { supported: true, discussions: [...discussions.values()], checkedAt, readState, nextCursor, knownPages, resetRequired: false };
}

export async function markAppDiscussionRead({ api, accountId, entry: value, conversationId, messageId, isCurrent }) {
  const entry = normalizeAppEntry(value);
  if (!CONVERSATION.test(conversationId) || !MESSAGE.test(messageId)) fail();
  if (!isCurrent()) return null;
  const response = await api.request('apps.discussion.read', { expectedAccountId: accountId, appId: entry.appId,
    domainId: entry.domainId, path: entry.path, conversationId, messageId });
  if (!isCurrent()) return null;
  if (response?.schema !== 'soty.app-discussion.read.v1' || response.appId !== entry.appId || response.conversationId !== conversationId
    || response.messageId !== messageId || !integer(response.unreadCount) || !integer(response.readAt)) fail();
  return { appId: entry.appId, conversationId, messageId, unreadCount: response.unreadCount, readAt: response.readAt };
}
