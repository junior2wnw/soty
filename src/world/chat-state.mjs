const PREFIX = 'soty.chat.draft.v1:';
const ATTACHMENT_PREFIX = 'soty.chat.draft.v2:';
const empty = () => ({ text: '', replyTo: null, pending: null });
const replyId = value => typeof value === 'string' && /^[A-Za-z0-9_-]{3,160}$/.test(value) ? value : null;
const refs = value => Array.isArray(value) && value.length <= 10 && value.every(item => replyId(item)) && new Set(value).size === value.length ? [...value] : [];
const withRefs = value => refs(value).length ? { attachments: refs(value) } : {};
const copy = value => ({ text: value.text, replyTo: value.replyTo, ...withRefs(value.attachments), pending: value.pending ? { ...value.pending, ...withRefs(value.pending.attachments) } : null });
function decode(raw) {
  const value = empty();
  try {
    const item = JSON.parse(raw || 'null');
    if (item && typeof item.text === 'string' && item.text.length <= 6000) {
      value.text = item.text;
      value.replyTo = replyId(item.replyTo);
      Object.assign(value, withRefs(item.attachments));
      if (item.pending && typeof item.pending.clientId === 'string' && /^[A-Za-z0-9_-]{3,160}$/.test(item.pending.clientId) && typeof item.pending.text === 'string' && item.pending.text.length <= 6000) value.pending = { clientId: item.pending.clientId, text: item.pending.text, replyTo: replyId(item.pending.replyTo) };
      if (value.pending) Object.assign(value.pending, withRefs(item.pending.attachments));
    }
  } catch { /* Invalid local metadata is not executable data. */ }
  return value;
}

/** Account-scoped local drafts retain the original request id across a lost ACK. */
export function createChatDraftStore(storage, makeId = () => crypto.randomUUID()) {
  const memory = new Map();
  const volatile = new Set();
  const attachmentScopes = new Set();
  const listeners = new Map();
  const keyFor = (accountId, communityId) => PREFIX + encodeURIComponent(accountId) + ':' + encodeURIComponent(communityId);
  function stored(key) {
    const newer = storage.getItem(ATTACHMENT_PREFIX + key.slice(PREFIX.length));
    if (newer !== null && newer !== undefined) { attachmentScopes.add(key); return newer; }
    return attachmentScopes.has(key) ? null : storage.getItem(key);
  }
  const notify = (key, value) => { for (const listener of listeners.get(key) || []) listener(copy(value)); };
  function read(accountId, communityId) {
    const key = keyFor(accountId, communityId);
    if (!memory.has(key)) {
      let value = empty();
      try { value = decode(stored(key)); } catch { /* The live session can still keep an in-memory draft. */ }
      memory.set(key, value);
    }
    return copy(memory.get(key));
  }
  function save(accountId, communityId, value) {
    const key = keyFor(accountId, communityId);
    memory.set(key, copy(value));
    let durable = true;
    try {
      // Attachment intents live in v2; an old tab cannot erase their refs/clientId.
      // Keep its small empty tombstone so legacy v1 text cannot revive after ACK.
      if (refs(value.attachments).length || refs(value.pending?.attachments).length) attachmentScopes.add(key);
      if (attachmentScopes.has(key)) storage.setItem(ATTACHMENT_PREFIX + key.slice(PREFIX.length), JSON.stringify(value));
      else if (value.text || value.replyTo || value.pending) storage.setItem(key, JSON.stringify(value));
      else storage.removeItem(key);
      volatile.delete(key);
    } catch {
      // A failed removal is a dirty tombstone too: the acknowledged/cleared
      // draft still exists on disk. Keep the live empty value and its warning
      // until retrySave confirms removal; stale storage events cannot revive it.
      durable = false; volatile.add(key);
    }
    notify(key, value); return durable;
  }
  return {
    read,
    edit(accountId, communityId, text, replyTo) {
      const value = read(accountId, communityId); value.text = text.slice(0, 6000);
      if (replyTo !== undefined) value.replyTo = replyId(replyTo);
      return save(accountId, communityId, value);
    },
    setAttachments(accountId, communityId, attachments) {
      const value=read(accountId,communityId);delete value.attachments;Object.assign(value,withRefs(attachments));
      return save(accountId,communityId,value);
    },
    beginSend(accountId, communityId, text, replyTo, attachments) {
      const key = keyFor(accountId, communityId);
      if (!volatile.has(key)) { try { memory.set(key, decode(stored(key))); } catch { /* Preserve the live draft. */ } }
      const value = read(accountId, communityId);
      value.text = text;
      if (replyTo !== undefined) value.replyTo = replyId(replyTo);
      if (attachments !== undefined) { delete value.attachments; Object.assign(value,withRefs(attachments)); }
      // An attachment send can already have committed even when its ACK was
      // lost. Keep that exact intent; edits are the next draft, not a new send.
      if (!value.pending) value.pending = { clientId: makeId(), text, replyTo: value.replyTo, ...withRefs(value.attachments) };
      const durable = save(accountId, communityId, value);
      return { ...value.pending, durable };
    },
    recoverPending(accountId, communityId, pending) {
      const value=read(accountId,communityId);
      const captured={clientId:pending.clientId,text:pending.text,replyTo:replyId(pending.replyTo),...withRefs(pending.attachments)};
      if (JSON.stringify(value.pending)===JSON.stringify(captured)) return value;
      value.pending=captured;save(accountId,communityId,value);return copy(value);
    },
    discardUnsubmitted(accountId, communityId, clientId) {
      const value=read(accountId,communityId);
      if(value.pending?.clientId===clientId){value.pending=null;save(accountId,communityId,value);}
      return value;
    },
    acknowledge(accountId, communityId, clientId, acceptedAttachments) {
      const key = keyFor(accountId, communityId);
      if (!volatile.has(key)) { try { memory.set(key, decode(stored(key))); } catch { /* A revoked storage permission must not erase the live copy. */ } }
      const value = read(accountId, communityId);
      // A late ACK must never clear a newer draft or a later send.
      const pending=value.pending?.clientId===clientId?value.pending:null;
      const accepted=refs(acceptedAttachments??pending?.attachments);
      if(!pending&&!refs(value.attachments).some(id=>accepted.includes(id))){notify(key,value);return value;}
      if (pending && value.text.trim() === pending.text && value.replyTo === pending.replyTo
        && JSON.stringify(refs(value.attachments)) === JSON.stringify(refs(pending.attachments))) { value.text = ''; value.replyTo = null; }
      const remaining=refs(value.attachments).filter(id=>!accepted.includes(id));delete value.attachments;Object.assign(value,withRefs(remaining));
      if(pending)value.pending=null;save(accountId, communityId, value);
      return copy(value);
    },
    retrySave(accountId, communityId) { return save(accountId, communityId, read(accountId, communityId)); },
    subscribe(accountId, communityId, listener) {
      const key = keyFor(accountId, communityId); const subscribers = listeners.get(key) || new Set(); subscribers.add(listener); listeners.set(key, subscribers);
      return () => { subscribers.delete(listener); if (!subscribers.size) listeners.delete(key); };
    },
    storageChanged(key) {
      const newer=key?.startsWith(ATTACHMENT_PREFIX);
      if (newer) key=PREFIX+key.slice(ATTACHMENT_PREFIX.length);
      if (!key?.startsWith(PREFIX) || volatile.has(key) || !newer&&attachmentScopes.has(key)) return;
      const [accountId, communityId] = key.slice(PREFIX.length).split(':');
      if (!accountId || !communityId) return;
      try { const value = decode(stored(key)); memory.set(key, value); notify(key, value); } catch { /* Keep the live copy if storage is no longer readable. */ }
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
