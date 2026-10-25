import { normalizeAppEntry } from './app-saved-state.mjs';
import { parseAppLaunchRoute } from './app-launch.mjs';

const CONVERSATION = /^conv_[a-f0-9]{32}$/u, MESSAGE = /^msg_[a-f0-9]{32}$/u;
const integer = value => Number.isSafeInteger(value) && value >= 0;
const text = (value, max) => typeof value === 'string' && value.length <= max && !/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/u.test(value);
const fail = () => { throw Object.assign(new Error('Invalid discussion inbox'), { code: 'app_discussion_invalid_inbox' }); };
export const appDiscussionInboxUnsupported = error => ['unknown_operation', 'unsupported_operation', 'capability_not_supported', 'operation_unavailable'].includes(error?.code);

export function recentAppDiscussionTargets(recent) {
  const targets = new Map();
  for (const value of Array.isArray(recent) ? recent.slice(0, 12) : []) {
    try {
      if (typeof value?.route !== 'string') continue;
      const intent = parseAppLaunchRoute(value.route.startsWith('#') ? value.route : `#${value.route}`);
      if (intent?.target.domainId && intent.target.path !== undefined && !targets.has(intent.target.appId)) targets.set(intent.target.appId, { ...intent.target });
    } catch { /* A malformed local recent entry is not a new authority or URL. */ }
  }
  return [...targets.values()];
}

/** Inputs are locations, not authority. Root independently checks every entry. */
export async function loadAppDiscussionInbox({ api, accountId, entries, isCurrent }) {
  const targets = new Map();
  for (const value of entries) { const entry = normalizeAppEntry(value); if (!targets.has(entry.appId)) targets.set(entry.appId, entry); }
  const source = [...targets.values()], discussions = [];
  let checkedAt = 0, readState = 'synced';
  for (let offset = 0; offset < source.length; offset += 50) {
    if (!isCurrent()) return null;
    const batch = source.slice(offset, offset + 50), expected = new Map(batch.map(entry => [entry.appId, entry]));
    let response;
    try { response = await api.request('apps.discussion.inbox', { expectedAccountId: accountId,
      entries: batch.map(({ appId, domainId, path }) => ({ appId, domainId, path })) }); }
    catch (error) { if (!isCurrent()) return null; if (appDiscussionInboxUnsupported(error)) return { supported: false, discussions: [], checkedAt: null }; throw error; }
    if (!isCurrent()) return null;
    if (response?.schema !== 'soty.app-discussion.inbox.v1' || !Array.isArray(response.discussions)
      || response.discussions.length > batch.length || !integer(response.checkedAt)) fail();
    const seen = new Set();
    for (const row of response.discussions) {
      const entry = normalizeAppEntry(row.entry), requested = expected.get(row.appId);
      if (!requested || seen.has(row.appId) || entry.appId !== row.appId || entry.domainId !== requested.domainId
        || entry.path !== requested.path || entry.origin !== requested.origin || !text(row.name, 160)
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
      discussions.push({ appId: row.appId, name: row.name, entry, conversationId: row.conversationId, lastMessage, unreadCount: row.unreadCount });
    }
    checkedAt = Math.max(checkedAt, response.checkedAt);
    if (response.readState === 'unavailable') readState = 'unavailable';
  }
  return { supported: true, discussions, checkedAt, readState };
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
