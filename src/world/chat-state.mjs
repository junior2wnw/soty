const PREFIX = 'soty.chat.draft.v1:';
const empty = () => ({ text: '', pending: null });
const copy = value => ({ text: value.text, pending: value.pending ? { ...value.pending } : null });
function decode(raw) {
  const value = empty();
  try {
    const item = JSON.parse(raw || 'null');
    if (item && typeof item.text === 'string' && item.text.length <= 6000) {
      value.text = item.text;
      if (item.pending && typeof item.pending.clientId === 'string' && /^[A-Za-z0-9_-]{3,160}$/.test(item.pending.clientId) && typeof item.pending.text === 'string' && item.pending.text.length <= 6000) value.pending = { clientId: item.pending.clientId, text: item.pending.text };
    }
  } catch { /* Invalid local metadata is not executable data. */ }
  return value;
}

/** Account-scoped local drafts retain the original request id across a lost ACK. */
export function createChatDraftStore(storage, makeId = () => crypto.randomUUID()) {
  const memory = new Map();
  const volatile = new Set();
  const listeners = new Map();
  const keyFor = (accountId, communityId) => PREFIX + encodeURIComponent(accountId) + ':' + encodeURIComponent(communityId);
  const notify = (key, value) => { for (const listener of listeners.get(key) || []) listener(copy(value)); };
  function read(accountId, communityId) {
    const key = keyFor(accountId, communityId);
    if (!memory.has(key)) {
      let value = empty();
      try { value = decode(storage.getItem(key)); } catch { /* The live session can still keep an in-memory draft. */ }
      memory.set(key, value);
    }
    return copy(memory.get(key));
  }
  function save(accountId, communityId, value) {
    const key = keyFor(accountId, communityId);
    memory.set(key, copy(value));
    let durable = true;
    try {
      if (value.text || value.pending) storage.setItem(key, JSON.stringify(value));
      else storage.removeItem(key);
      volatile.delete(key);
    } catch { durable = false; if (value.text || value.pending) volatile.add(key); else volatile.delete(key); }
    notify(key, value); return durable;
  }
  return {
    read,
    edit(accountId, communityId, text) {
      const value = read(accountId, communityId); value.text = text.slice(0, 6000);
      return save(accountId, communityId, value);
    },
    beginSend(accountId, communityId, text) {
      const value = read(accountId, communityId);
      value.text = text;
      if (value.pending?.text !== text) value.pending = { clientId: makeId(), text };
      const durable = save(accountId, communityId, value);
      return { ...value.pending, durable };
    },
    acknowledge(accountId, communityId, clientId) {
      const key = keyFor(accountId, communityId);
      if (!volatile.has(key)) { try { memory.set(key, decode(storage.getItem(key))); } catch { /* A revoked storage permission must not erase the live copy. */ } }
      const value = read(accountId, communityId);
      // A late ACK must never clear a newer draft or a later send.
      if (value.pending?.clientId !== clientId) return value;
      if (value.text.trim() === value.pending.text) value.text = '';
      value.pending = null; save(accountId, communityId, value);
      return copy(value);
    },
    retrySave(accountId, communityId) { return save(accountId, communityId, read(accountId, communityId)); },
    subscribe(accountId, communityId, listener) {
      const key = keyFor(accountId, communityId); const subscribers = listeners.get(key) || new Set(); subscribers.add(listener); listeners.set(key, subscribers);
      return () => { subscribers.delete(listener); if (!subscribers.size) listeners.delete(key); };
    },
    storageChanged(key) {
      if (!key?.startsWith(PREFIX) || volatile.has(key)) return;
      const [accountId, communityId] = key.slice(PREFIX.length).split(':');
      if (!accountId || !communityId) return;
      try { const value = decode(storage.getItem(key)); memory.set(key, value); notify(key, value); } catch { /* Keep the live copy if storage is no longer readable. */ }
    },
    hasVolatile() { return volatile.size > 0; },
    isVolatile(accountId, communityId) { return volatile.has(keyFor(accountId, communityId)); },
  };
}

/** Bounded catch-up uses a fetched cursor, never an optimistic send's sequence. */
export async function readChatForward({ after, fetchPage, append, active = () => true, pageBudget = 4 }) {
  let cursor = after, hasMore = false;
  for (let page = 0; page < pageBudget && active(); page++) {
    const response = await fetchPage(cursor);
    if (!active()) return { cursor: after, hasMore: false };
    const rows = response.messages.filter(message => message.seq > cursor).sort((a, b) => a.seq - b.seq);
    for (const message of rows) { append(message); cursor = Math.max(cursor, message.seq); }
    hasMore = response.hasMore && rows.length > 0;
    if (!hasMore) break;
  }
  return { cursor, hasMore };
}
