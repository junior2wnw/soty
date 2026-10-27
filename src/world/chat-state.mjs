const PREFIX = 'soty.chat.draft.v1:';
const ATTACHMENT_PREFIX = 'soty.chat.draft.v2:';
const IDB_PREFIX = 'soty.chat.draft.v3:';
const IDB_WRITER = 'soty.chat.idb.v1';
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
      if (value.pending && item.pending.preserveDraft === true) value.pending.preserveDraft = true;
    }
  } catch { /* Invalid local metadata is not executable data. */ }
  return value;
}
function legacyCandidate(raw,source) {
  try {const item=JSON.parse(raw||'null');if(!item||item._sotyWriter===IDB_WRITER||typeof item.text!=='string'||item.text.length>6000)return null;
    const replyTo=replyId(item.replyTo);return item.text||replyTo?{source,text:item.text,replyTo}:null;
  }catch{return null;}
}

/** Account-scoped local drafts retain the original request id across a lost ACK. */
export function createChatDraftStore(storage, makeId = () => crypto.randomUUID(), {indexedDB=false} = {}) {
  const memory = new Map();
  const volatile = new Set();
  const attachmentScopes = new Set();
  const listeners = new Map();
  const keyFor = (accountId, communityId) => PREFIX + encodeURIComponent(accountId) + ':' + encodeURIComponent(communityId);
  function stored(key) {
    if(indexedDB){const current=storage.getItem(IDB_PREFIX+key.slice(PREFIX.length));if(current!==null&&current!==undefined)return current;}
    const newer = storage.getItem(ATTACHMENT_PREFIX + key.slice(PREFIX.length));
    if (newer !== null && newer !== undefined) { attachmentScopes.add(key); return newer; }
    return attachmentScopes.has(key) ? null : storage.getItem(key);
  }
  const notify = (key, value, legacy) => { for (const listener of listeners.get(key) || []) listener(copy(value),legacy); };
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
      // Older clients keep attachment intents in v2. Modern clients use their
      // own v3 cache and never overwrite or remove either old storage key.
      if (refs(value.attachments).length || refs(value.pending?.attachments).length) attachmentScopes.add(key);
      const encoded=JSON.stringify(indexedDB?{...value,_sotyWriter:IDB_WRITER}:value);
      if (indexedDB) storage.setItem(IDB_PREFIX + key.slice(PREFIX.length),encoded);
      else if (attachmentScopes.has(key)) storage.setItem(ATTACHMENT_PREFIX + key.slice(PREFIX.length), encoded);
      else if (value.text || value.replyTo || value.pending) storage.setItem(key, encoded);
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
    legacyCandidates(accountId,communityId) {
      const key=keyFor(accountId,communityId),suffix=key.slice(PREFIX.length);
      const readCandidate=(name,source)=>{try{return legacyCandidate(storage.getItem(name),source);}catch{return null;}};
      return [readCandidate(key,'v1'),readCandidate(ATTACHMENT_PREFIX+suffix,'v2')].filter(Boolean);
    },
    legacyCurrent(accountId,communityId,source) {
      const key=keyFor(accountId,communityId),name=source==='v2'?ATTACHMENT_PREFIX+key.slice(PREFIX.length):key;
      try{return legacyCandidate(storage.getItem(name),source);}catch{return undefined;}
    },
    edit(accountId, communityId, text, replyTo) {
      const value = read(accountId, communityId); value.text = text.slice(0, 6000);
      if (value.pending) value.pending.preserveDraft = true;
      if (replyTo !== undefined) value.replyTo = replyId(replyTo);
      return save(accountId, communityId, value);
    },
    setAttachments(accountId, communityId, attachments) {
      const value=read(accountId,communityId);delete value.attachments;Object.assign(value,withRefs(attachments));
      return save(accountId,communityId,value);
    },
    beginSend(accountId, communityId, text, replyTo, attachments) {
      const key = keyFor(accountId, communityId);
      const observed = read(accountId, communityId).pending;
      if (!volatile.has(key)) { try { memory.set(key, decode(stored(key))); } catch { /* Preserve the live draft. */ } }
      const value = read(accountId, communityId);
      value.text = text;
      if (replyTo !== undefined) value.replyTo = replyId(replyTo);
      if (attachments !== undefined) { delete value.attachments; Object.assign(value,withRefs(attachments)); }
      // An attachment send can already have committed even when its ACK was
      // lost. Keep that exact intent; edits are the next draft, not a new send.
      const reusedOther = Boolean(value.pending && value.pending.clientId !== observed?.clientId);
      if (reusedOther) value.pending.preserveDraft = true;
      if (!value.pending) value.pending = { clientId: makeId(), text, replyTo: value.replyTo, ...withRefs(value.attachments) };
      const durable = save(accountId, communityId, value);
      return { ...value.pending, durable, ...(reusedOther ? { reusedOther: true } : {}) };
    },
    recoverPending(accountId, communityId, pending) {
      const value=read(accountId,communityId);
      const captured={clientId:pending.clientId,text:pending.text,replyTo:replyId(pending.replyTo),...withRefs(pending.attachments)};
      if (value.pending?.clientId !== captured.clientId || value.pending?.preserveDraft) captured.preserveDraft=true;
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
      if (pending && !pending.preserveDraft && value.text.trim() === pending.text && value.replyTo === pending.replyTo
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
    storageChanged(key, newValue) {
      const eventKey=key;
      const current=key?.startsWith(IDB_PREFIX);
      const newer=key?.startsWith(ATTACHMENT_PREFIX);
      let candidate=current||newValue===undefined?null:legacyCandidate(newValue,newer?'v2':'v1');
      if(current)key=PREFIX+key.slice(IDB_PREFIX.length);
      if (newer) key=PREFIX+key.slice(ATTACHMENT_PREFIX.length);
      if (!key?.startsWith(PREFIX)) return;
      if(indexedDB&&!current){if(candidate){try{const latest=storage.getItem(eventKey);if(latest!==newValue)candidate=legacyCandidate(latest,newer?'v2':'v1');}catch{/* Keep the event if storage is unavailable. */}}
        if(candidate)notify(key,memory.get(key)??empty(),{...candidate,fresh:true});return;}
      if(!indexedDB&&current)return;
      if (volatile.has(key) || !indexedDB&&!newer&&attachmentScopes.has(key)) {if(candidate)notify(key,memory.get(key)??empty(),candidate);return;}
      const [accountId, communityId] = key.slice(PREFIX.length).split(':');
      if (!accountId || !communityId) return;
      try { const value = decode(stored(key)); memory.set(key, value); notify(key, value,candidate); } catch { if(candidate)notify(key,memory.get(key)??empty(),candidate); }
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
