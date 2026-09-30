const id = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{3,180}$/u.test(value);
const copy = value => structuredClone(value);
const text = (value, limit) => typeof value === 'string' ? value.slice(0, limit) : '';
const lastJob = value => value && id(value.hostDeviceId) && id(value.connectorId) && id(value.jobId)
  ? { hostDeviceId: value.hostDeviceId, connectorId: value.connectorId, jobId: value.jobId } : null;
function payload(value) {
  if (!value || !id(value.hostDeviceId) || !id(value.connectorId) || !id(value.conversationId)
    || !id(value.expectedAccountId) || (value.previousJobId !== undefined && !id(value.previousJobId))) throw new Error('invalid_assistant_target');
  const prompt = text(value.text, 16_001).trim(), cwd = text(value.cwd, 2001).trim();
  if (!prompt || prompt.length > 16_000 || cwd.length > 2000 || /[\u0000-\u001f]/u.test(cwd)) throw new Error('invalid_assistant_prompt');
  return { expectedAccountId: value.expectedAccountId, hostDeviceId: value.hostDeviceId, connectorId: value.connectorId,
    conversationId: value.conversationId, text: prompt, cwd, ...(value.previousJobId ? { previousJobId: value.previousJobId } : {}) };
}

/** Each conversation owns a durable draft and immutable pending request. The
 * active conversation belongs to this tab: another tab cannot retarget it. */
export function createAssistantState({ accountId, storage, tabStorage, randomId = () => crypto.randomUUID() }) {
  if (!id(accountId)) throw new Error('invalid_account');
  const indexKey = `soty.assistant.v2:${accountId}`, legacyKey = `soty.assistant.v1:${accountId}`;
  const recordKey = value => `${indexKey}:${value}`;
  const fresh = (conversationId = randomId()) => ({ text: '', cwd: '', deviceKey: '', conversationId, previousJobId: '', lastJob: null, pending: null });
  let state = fresh(), activeId = state.conversationId, unsaved = false, readable = true;
  const parse = (raw, expected) => {
    if (raw.length > 80_000) throw new Error('invalid_draft');
    const parsed = JSON.parse(raw);
    if (!parsed || !id(parsed.conversationId) || (expected && parsed.conversationId !== expected)) throw new Error('invalid_draft');
    let pending = null;
    if (parsed.pending) {
      const value = payload(parsed.pending.payload);
      if (!id(parsed.pending.requestId) || value.expectedAccountId !== accountId || value.conversationId !== parsed.conversationId) throw new Error('invalid_pending');
      pending = { requestId: parsed.pending.requestId, payload: value };
    }
    return { text: text(parsed.text, 16_000), cwd: text(parsed.cwd, 2000), deviceKey: text(parsed.deviceKey, 400),
      conversationId: parsed.conversationId, previousJobId: id(parsed.previousJobId) ? parsed.previousJobId : '', lastJob: lastJob(parsed.lastJob), pending };
  };
  function index() {
    const raw = storage.getItem(indexKey);
    if (!raw) return { current: '', conversations: [] };
    if (raw.length > 30_000) throw new Error('invalid_draft_index');
    const value = JSON.parse(raw);
    if (!id(value?.current) || !Array.isArray(value.conversations) || value.conversations.length > 128
      || !value.conversations.every(id) || new Set(value.conversations).size !== value.conversations.length) throw new Error('invalid_draft_index');
    return value;
  }
  try {
    const pointer = index();
    const tabId = tabStorage?.getItem(indexKey);
    if (tabId && !id(tabId)) throw new Error('invalid_tab_draft');
    if (tabId) pointer.current = tabId;
    const raw = pointer.current ? storage.getItem(recordKey(pointer.current)) : storage.getItem(legacyKey);
    if (raw) { state = parse(raw, pointer.current); activeId = state.conversationId; tabStorage?.setItem(indexKey, activeId); }
    else if (pointer.current) throw new Error('missing_draft');
  } catch { readable = false; }
  function read() {
    if (unsaved || !readable) return copy(state);
    try { const raw = storage.getItem(recordKey(activeId)); if (raw) state = parse(raw, activeId); }
    catch { readable = false; }
    return copy(state);
  }
  function persist(next) {
    if (!readable) throw new Error('assistant_storage_unavailable');
    const pointer = index();
    const retained = pointer.conversations.filter(value => {
      if (value === next.conversationId) return true;
      const raw = storage.getItem(recordKey(value));
      if (!raw) throw new Error('missing_draft');
      const draft = parse(raw, value); return Boolean(draft.text.trim() || draft.pending);
    });
    const conversations = retained.includes(next.conversationId) ? retained : [...retained, next.conversationId];
    if (conversations.length > 128) throw new Error('assistant_draft_limit');
    // Write the full draft first. Failed pointer storage cannot erase it, and
    // dispatch stays blocked until both writes have succeeded.
    storage.setItem(recordKey(next.conversationId), JSON.stringify(next));
    storage.setItem(indexKey, JSON.stringify({ current: next.conversationId, conversations }));
    tabStorage?.setItem(indexKey, next.conversationId);
  }
  function write(next) {
    state = copy(next);
    try { persist(state); unsaved = false; } catch { unsaved = true; }
    return !unsaved;
  }
  return {
    get key() { return recordKey(activeId); }, read,
    listDrafts() {
      const result = [];
      try {
        for (const conversationId of new Set([...index().conversations, activeId])) {
          const raw = storage.getItem(recordKey(conversationId));
          const value = conversationId === activeId ? read() : raw ? parse(raw, conversationId) : null;
          if (value && (value.text.trim() || value.pending)) { const { pending, ...draft } = value; result.push({ ...draft, text: draft.text || pending?.payload.text || '' }); }
        }
      } catch { /* The active local draft remains usable without storage. */ }
      return result;
    },
    saveDraft(patch) {
      const next = read();
      for (const [field, limit] of [['text', 16_000], ['cwd', 2000], ['deviceKey', 400]]) if (Object.hasOwn(patch, field)) next[field] = text(patch[field], limit);
      write(next); return copy(state);
    },
    prepare(value) {
      const next = read(), input = payload(value);
      if (!readable) throw new Error('assistant_storage_unavailable');
      if (input.expectedAccountId !== accountId || input.conversationId !== activeId) throw new Error('invalid_account');
      if (next.pending) {
        if (JSON.stringify(next.pending.payload) !== JSON.stringify(input)) throw new Error('assistant_pending_unconfirmed');
      } else next.pending = { requestId: randomId(), payload: input };
      if (!write(next)) throw new Error('assistant_storage_unavailable');
      return copy(next.pending);
    },
    acknowledge(requestId, job) {
      const next = read(); if (next.pending?.requestId !== requestId) return false;
      if (!id(job.jobId) || job.hostDeviceId !== next.pending.payload.hostDeviceId || job.connectorId !== next.pending.payload.connectorId) throw new Error('invalid_assistant_receipt');
      if (next.text.trim() === next.pending.payload.text) next.text = '';
      next.lastJob = copy(job); next.pending = null; next.previousJobId = '';
      write(next); return true;
    },
    restore(conversationId) {
      const previous = read();
      if (previous.pending) throw new Error('assistant_pending_unconfirmed');
      if (!id(conversationId)) throw new Error('invalid_assistant_target');
      if (!write(previous)) throw new Error('assistant_storage_unavailable');
      const raw = storage.getItem(recordKey(conversationId));
      if (!raw) throw new Error('assistant_draft_unavailable');
      const next = parse(raw, conversationId);
      persist(next); activeId = conversationId; state = next; unsaved = false; return copy(state);
    },
    select({ conversationId, previousJobId = '', lastJob: job = null, cwd = '', deviceKey = '' }) {
      const previous = read();
      if (previous.pending) throw new Error('assistant_pending_unconfirmed');
      if (!id(conversationId)) throw new Error('invalid_assistant_target');
      if (!write(previous)) throw new Error('assistant_storage_unavailable');
      let next = previous;
      if (activeId !== conversationId) { const raw = storage.getItem(recordKey(conversationId)); next = raw ? parse(raw, conversationId) : fresh(conversationId); }
      if (!next.pending) next = { ...next, previousJobId, lastJob: lastJob(job), cwd, deviceKey };
      persist(next); activeId = conversationId; state = next; unsaved = false; return copy(state);
    },
    forgetRejected(requestId) {
      const next = read(); if (next.pending?.requestId !== requestId) return false;
      next.pending = null; write(next); return true;
    },
    flush() { if (unsaved && !write(state)) throw new Error('assistant_storage_unavailable'); },
    hasUnsavedChanges() { return unsaved; },
  };
}
