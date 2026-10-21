// A conversation is a view of an existing server entity. Direct threads never
// acquire community roles, membership, invitations or application grants here.
const fail = code => { throw Object.assign(new Error(code), { code }); };
const actions = new Set(['list', 'send', 'remove', 'read']);
const closedAction = action => { if (!actions.has(action)) fail('unsupported_chat_action'); };

export function createCommunityConversation({ api, community, onUpdate = () => {}, isCurrent = () => true }) {
  const id = community.communityId;
  let current = community, projectionEpoch = 0, unreadEpoch = 0, latestRead = 0, preferencesQueue = Promise.resolve();
  const accept = next => {
    if (!isCurrent()) fail('conversation_changed');
    if (!next || next.communityId !== id || next.membership?.state !== 'active') fail('community_membership_required');
    const metadata = current.revision > next.revision ? current : next;
    const membership = current.membership.revision > next.membership.revision ? current : next;
    projectionEpoch++; current = { ...metadata, membership: membership.membership, permissions: membership.permissions, unreadCount: next.unreadCount }; onUpdate(current);
  };
  return Object.freeze({
    kind: 'community', id, draftKey: id, avatarContext: id,
    get record() { return current; },
    get name() { return current.name; },
    get subtitle() { return current.memberCount; },
    get canWrite() { return current.permissions.canWrite; },
    get canModerate() { return current.permissions.canModerate; },
    get preferences() { return current.membership; },
    acceptCommunity: accept,
    async refresh() {
      const epoch = projectionEpoch, read = ++latestRead;
      const result = await api.request('world.community.get', { communityId: id });
      if (epoch === projectionEpoch && read === latestRead) accept(result.community);
    },
    request(action, params = {}) { closedAction(action); return api.request('world.chat.' + action, { ...params, communityId: id }); },
    async setPreference(key, value) {
      if (!['muted', 'pinned'].includes(key) || typeof value !== 'boolean') fail('invalid_chat_preference');
      projectionEpoch++;
      const writing = preferencesQueue.then(async () => {
        if (!isCurrent()) fail('conversation_changed');
        const readAtDispatch = unreadEpoch;
        const result = await api.request('world.membership.preferences', { communityId: id, [key]: value });
        accept(readAtDispatch === unreadEpoch ? result.community : { ...result.community, unreadCount: current.unreadCount });
      });
      preferencesQueue = writing.catch(() => {}); await writing;
    },
    setUnread(count) { unreadEpoch++; projectionEpoch++; current.unreadCount = count; onUpdate(current); },
  });
}

export function createDirectConversation({ api, thread, accountId, peerLabel = '', onUpdate = () => {}, isCurrent = () => true }) {
  const id = thread.threadId, relationshipId = thread.relationshipId, peerId = thread.peer.accountId;
  // A newly accepted relationship needs a new explicit open. A prior draft or
  // pending clientId must never be rebound to it by an inbox refresh.
  const scope = Object.freeze({ expectedAccountId: accountId, threadId: id, expectedRelationshipId: relationshipId });
  let current = thread, projectionEpoch = 0, unreadEpoch = 0, latestRead = 0, preferencesQueue = Promise.resolve();
  const name = () => current.peer.label || (typeof peerLabel === 'function' ? peerLabel() : peerLabel) || 'Контакт';
  const accept = next => {
    if (!isCurrent()) fail('conversation_changed');
    if (!next || next.threadId !== id || next.relationshipId !== relationshipId || next.peer?.accountId !== peerId) fail('direct_not_found');
    projectionEpoch++; current = next; onUpdate(current);
  };
  const messages = result => ({ ...result, ...(result.messages ? { messages: result.messages.map(message) } : {}), ...(result.message ? { message: message(result.message) } : {}) });
  const message = value => ({ ...value, author: {
    ...value.author, displayName: value.author.displayName || (value.author.profileId === accountId ? 'Вы' : name()),
    avatarColor: value.author.avatarColor || '#C89448', revision: value.author.revision ?? 0,
  } });
  return Object.freeze({
    kind: 'direct', id, draftKey: `direct:${id}:${relationshipId}`, avatarContext: undefined,
    get record() { return current; }, get name() { return name(); }, get subtitle() { return 'Личный разговор'; },
    get canWrite() { return true; }, get canModerate() { return false; }, get preferences() { return current.preferences; },
    async refresh() {
      const epoch = projectionEpoch, read = ++latestRead;
      const result = await api.request('world.direct.get', scope);
      if (epoch === projectionEpoch && read === latestRead) accept(result.thread);
    },
    async request(action, params = {}) {
      closedAction(action); const result = await api.request('world.direct.chat.' + action, { ...params, ...scope }); return messages(result);
    },
    async setPreference(key, value) {
      if (!['muted', 'pinned'].includes(key) || typeof value !== 'boolean') fail('invalid_chat_preference');
      projectionEpoch++;
      const writing = preferencesQueue.then(async () => {
        if (!isCurrent()) fail('conversation_changed');
        const readAtDispatch = unreadEpoch;
        const result = await api.request('world.direct.preferences', { ...scope, [key]: value });
        accept(readAtDispatch === unreadEpoch ? result.thread : { ...result.thread, unreadCount: current.unreadCount });
      });
      preferencesQueue = writing.catch(() => {}); await writing;
    },
    setUnread(count) { unreadEpoch++; projectionEpoch++; current.unreadCount = count; onUpdate(current); },
  });
}
