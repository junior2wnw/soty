import { normalizeAppEntry } from './app-saved-state.mjs';

const APP = /^app-[a-f0-9]{32}$/u, CONV = /^conv_[a-f0-9]{32}$/u, MESSAGE = /^msg_[a-f0-9]{32}$/u;
const id = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,180}$/u.test(value);
const integer = value => Number.isSafeInteger(value) && value >= 0;
const object = value => value && typeof value === 'object' && !Array.isArray(value);
const clone = value => structuredClone(value), same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const fail = code => { throw Object.assign(new Error(code), { code }); };
const check = (ok, code = 'app_discussion_invalid_data') => { if (!ok) fail(code); };
const size = value => new TextEncoder().encode(JSON.stringify(value)).byteLength;
const keyFor = accountId => `soty.app-discussion.drafts.v1:${accountId}`;
const bus = new Map(), notify = key => { for (const listener of bus.get(key) ?? []) listener(); };
function text(value, allowEmpty = true) {
  check(typeof value === 'string' && value.length <= 4000 && value.isWellFormed()
    && !/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/u.test(value)
    && (allowEmpty || value.trim().length > 0), 'app_discussion_invalid_text'); return value;
}
function messageId(value) { check(typeof value === 'string' && MESSAGE.test(value)); return value; }
function scopeValue(value) {
  check(object(value) && id(value.accountId) && typeof value.appId === 'string' && APP.test(value.appId)
    && typeof value.conversationId === 'string' && CONV.test(value.conversationId), 'app_discussion_invalid_scope');
  const entry = normalizeAppEntry(value.entry); check(entry.appId === value.appId, 'app_discussion_invalid_scope');
  return { accountId: value.accountId, appId: value.appId, conversationId: value.conversationId, entry };
}
function draftValue(value) {
  check(object(value) && id(value.revision));
  return { text: text(value.text), replyTo: value.replyTo === null ? null : messageId(value.replyTo), revision: value.revision };
}
function pendingValue(value, expectedScope) {
  check(object(value) && object(value.args) && id(value.draftRevision), 'app_discussion_invalid_pending');
  const scope = scopeValue(value.scope), args = value.args;
  check((!expectedScope || same(scope, expectedScope)) && args.expectedAccountId === scope.accountId && args.appId === scope.appId
    && args.conversationId === scope.conversationId && args.domainId === scope.entry.domainId && args.path === scope.entry.path
    && id(args.requestId) && Object.keys(args).every(key => ['expectedAccountId', 'appId', 'conversationId', 'domainId', 'path', 'requestId', 'body', 'replyTo'].includes(key)), 'app_discussion_invalid_pending');
  return { scope, draftRevision: value.draftRevision, args: { expectedAccountId: scope.accountId, appId: scope.appId, conversationId: scope.conversationId,
    domainId: scope.entry.domainId, path: scope.entry.path, requestId: args.requestId, body: text(args.body, false),
    ...(args.replyTo === undefined ? {} : { replyTo: messageId(args.replyTo) }) } };
}
function loadRecord(storage, accountId) {
  try {
    const raw = storage.getItem(keyFor(accountId));
    if (raw === null) return { schema: 1, accountId, revision: 0, slots: [] };
    check(typeof raw === 'string' && new TextEncoder().encode(raw).byteLength <= 1024 * 1024);
    const value = JSON.parse(raw); check(value?.schema === 1 && value.accountId === accountId && integer(value.revision)
      && Array.isArray(value.slots) && value.slots.length <= 32);
    const slots = value.slots.map(item => {
      const scope = scopeValue(item.scope); check(scope.accountId === accountId);
      return { scope, draft: draftValue(item.draft), pending: item.pending === null ? null : pendingValue(item.pending, scope) };
    });
    check(new Set(slots.map(item => JSON.stringify(item.scope))).size === slots.length);
    return { schema: 1, accountId, revision: value.revision, slots };
  } catch { fail('app_discussion_storage_unavailable'); }
}
function writeRecord(storage, record) {
  check(record.slots.length <= 32 && size(record) <= 1024 * 1024, 'app_discussion_local_capacity');
  try { const raw = JSON.stringify(record); storage.setItem(keyFor(record.accountId), raw); check(storage.getItem(keyFor(record.accountId)) === raw); }
  catch { fail('app_discussion_storage_unavailable'); }
}
function putSlot(record, scope, draft, pending) {
  const slots = record.slots.filter(item => !same(item.scope, scope));
  if (draft.text || draft.replyTo || pending) slots.push({ scope, draft, pending });
  check(record.revision < Number.MAX_SAFE_INTEGER, 'app_discussion_local_revision_exhausted');
  return { ...record, revision: record.revision + 1, slots };
}
export function listAppDiscussionDrafts({ accountId, appId, domainId, path, origin, storage }) {
  check(id(accountId) && typeof appId === 'string' && APP.test(appId) && typeof domainId === 'string' && /^dom_[a-f0-9]{32}$/u.test(domainId)
    && typeof path === 'string' && (origin === undefined || typeof origin === 'string'), 'app_discussion_invalid_scope');
  return clone(loadRecord(storage, accountId).slots.filter(item => item.scope.appId === appId && item.scope.entry.domainId === domainId
    && item.scope.entry.path === path && (origin === undefined || item.scope.entry.origin === origin)));
}

export function createAppDiscussionDraftState({ scope: source, storage, locks, randomId = () => crypto.randomUUID() }) {
  const scope = scopeValue(source), key = keyFor(scope.accountId), listeners = new Set();
  const fresh = () => { const revision = randomId(); check(id(revision)); return { text: '', replyTo: null, revision }; };
  let draft = fresh(), pending = null, baseRevision = null, dirty = false, durable = true, conflict = false, remoteDraft = null,
    error = null, disposed = false, flushing = null;
  const slotOf = record => record.slots.find(item => same(item.scope, scope));
  const emit = () => { if (!disposed) for (const listener of listeners) listener(); };
  const locked = callback => locks?.request ? locks.request(key, callback)
    : Promise.reject(Object.assign(new Error('app_discussion_lock_unavailable'), { code: 'app_discussion_lock_unavailable' }));
  const recordFailure = reason => { durable = false; error = reason?.code ?? 'app_discussion_storage_unavailable'; emit(); };
  function observe(slot) {
    pending = clone(slot?.pending ?? null);
    const revision = slot?.draft.revision ?? null;
    if (dirty || conflict) {
      conflict = revision !== baseRevision;
      remoteDraft = conflict ? clone(slot?.draft ?? null) : null;
    } else {
      draft = clone(slot?.draft ?? fresh()); baseRevision = revision; remoteDraft = null;
      durable = true; error = null;
    }
  }
  function refreshLocal() {
    try { observe(slotOf(loadRecord(storage, scope.accountId))); } catch (reason) { recordFailure(reason); }
    emit();
  }
  refreshLocal();
  if (!bus.has(key)) bus.set(key, new Set()); bus.get(key).add(refreshLocal);
  function checkBase(slot) {
    const revision = slot?.draft.revision ?? null;
    if (revision !== baseRevision) { conflict = true; remoteDraft = clone(slot?.draft ?? null); pending = clone(slot?.pending ?? null);
      durable = false; error = 'app_discussion_draft_conflict'; fail(error); }
  }
  function persist(record, selectedDraft, selectedPending) {
    const next = putSlot(record, scope, selectedDraft, selectedPending); writeRecord(storage, next);
    const stored = slotOf(next);
    baseRevision = stored?.draft.revision ?? null; pending = clone(stored?.pending ?? null);
    draft = clone(selectedDraft); dirty = false; durable = true; conflict = false; remoteDraft = null; error = null;
    notify(key); emit();
  }
  async function transition(callback) {
    try { return await locked(callback); }
    catch (reason) { recordFailure(reason); throw reason; }
  }
  const state = {
    key, scope: clone(scope), read: () => clone({ draft, pending, durable, conflict, remoteDraft, error }),
    readRetained: () => listAppDiscussionDrafts({ accountId: scope.accountId, ...scope.entry, storage }).filter(item => item.scope.conversationId !== scope.conversationId),
    subscribe(listener) { listeners.add(listener); return () => listeners.delete(listener); }, refreshLocal,
    dispose() { disposed = true; listeners.clear(); bus.get(key)?.delete(refreshLocal); if (!bus.get(key)?.size) bus.delete(key); },
    edit(value) {
      check(!disposed, 'app_discussion_closed'); const next = { text: text(value.text), replyTo: value.replyTo === null ? null : messageId(value.replyTo) };
      if (next.text === draft.text && next.replyTo === draft.replyTo) return;
      draft = { ...next, revision: fresh().revision }; dirty = true; durable = false; emit();
    },
    hasUnsavedChanges: () => dirty || !durable || conflict,
    flush() {
      if (flushing) return flushing;
      flushing = transition(() => {
        const record = loadRecord(storage, scope.accountId), slot = slotOf(record); checkBase(slot);
        if (!dirty && durable) { pending = clone(slot?.pending ?? null); return true; }
        persist(record, draft, slot?.pending ?? null); return true;
      }).catch(() => false).finally(() => { flushing = null; });
      return flushing;
    },
    async prepareSend(expectedDraft, beforeCreate) {
      const expected = draftValue(expectedDraft); check(same(expected, draft), 'app_discussion_draft_changed'); text(expected.text, false);
      check(typeof beforeCreate === 'function', 'app_discussion_context_changed');
      return transition(() => {
        const record = loadRecord(storage, scope.accountId), slot = slotOf(record); checkBase(slot);
        check(!slot?.pending, 'app_discussion_pending_unconfirmed');
        const current = beforeCreate(); if (current && typeof current.then === 'function') Promise.resolve(current).catch(() => {});
        check(current === true, 'app_discussion_context_changed');
        const requestId = randomId(); check(id(requestId));
        const value = pendingValue({ scope, draftRevision: expected.revision, args: { expectedAccountId: scope.accountId, appId: scope.appId,
          conversationId: scope.conversationId, domainId: scope.entry.domainId, path: scope.entry.path, requestId, body: expected.text,
          ...(expected.replyTo === null ? {} : { replyTo: expected.replyTo }) } }, scope);
        // The user may already be typing the next text while this short lock
        // was queued. Persist that newer draft, but send the clicked snapshot.
        persist(record, draft, value); return clone(value);
      });
    },
    async pendingForDispatch(expectedPending) {
      check(expectedPending, 'app_discussion_pending_changed'); const expected = pendingValue(expectedPending, scope);
      return transition(() => { const slot = slotOf(loadRecord(storage, scope.accountId));
        check(slot?.pending && same(expected, slot.pending), 'app_discussion_pending_changed'); return clone(slot.pending); });
    },
    async acknowledge(expectedPending, response) {
      const expected = pendingValue(expectedPending, scope); verifySend(expected, response);
      return transition(() => {
        const record = loadRecord(storage, scope.accountId), slot = slotOf(record);
        if (!slot?.pending || !same(slot.pending, expected)) return false;
        const selected = slot.draft.revision === expected.draftRevision ? fresh() : slot.draft;
        const next = putSlot(record, scope, selected, null); writeRecord(storage, next);
        if (baseRevision === slot.draft.revision) {
          baseRevision = slotOf(next)?.draft.revision ?? null;
          if (!dirty) draft = clone(selected);
        }
        pending = null; if (!dirty) { durable = true; error = null; }
        notify(key); emit(); return true;
      });
    },
    async abandon(expectedPending) {
      const expected = pendingValue(expectedPending, scope);
      return transition(() => {
        const record = loadRecord(storage, scope.accountId), slot = slotOf(record);
        if (!slot?.pending || !same(slot.pending, expected)) return false;
        writeRecord(storage, putSlot(record, scope, slot.draft, null)); pending = null; notify(key); emit(); return true;
      });
    },
    async discardDraft(expectedDraft) {
      const expected = draftValue(expectedDraft);
      return transition(() => {
        const record = loadRecord(storage, scope.accountId), slot = slotOf(record);
        if (!slot || !same(slot.draft, expected)) return false;
        const blank = fresh(), next = putSlot(record, scope, blank, slot.pending); writeRecord(storage, next);
        if (same(draft, expected)) { draft = blank; dirty = false; durable = true; baseRevision = slotOf(next)?.draft.revision ?? null; }
        notify(key); emit(); return true;
      });
    },
    async chooseRemote(expectedRemoteRevision) {
      return transition(() => {
        const slot = slotOf(loadRecord(storage, scope.accountId)); check((slot?.draft.revision ?? null) === expectedRemoteRevision, 'app_discussion_draft_changed');
        dirty = false; conflict = false; observe(slot); emit(); return true;
      });
    },
    async keepMine(expectedRemoteRevision) {
      return transition(() => {
        const record = loadRecord(storage, scope.accountId), slot = slotOf(record);
        check((slot?.draft.revision ?? null) === expectedRemoteRevision, 'app_discussion_draft_changed');
        persist(record, draft, slot?.pending ?? null); return true;
      });
    },
  };
  return state;
}

function messageValue(value, conversationId) {
  check(object(value) && value.conversationId === conversationId && object(value.author) && id(value.author.accountId)
    && typeof value.author.label === 'string' && value.author.label.trim() === value.author.label && value.author.label.length > 0
    && value.author.label.length <= 80 && value.author.label.isWellFormed() && !/[\u0000-\u001f\u007f]/u.test(value.author.label)
    && integer(value.createdAt) && (value.removedAt === null || integer(value.removedAt)) && typeof value.canRemove === 'boolean');
  const body = value.body === null ? null : text(value.body, false);
  check((body === null) === (value.removedAt !== null));
  return { id: messageId(value.id), conversationId, author: { accountId: value.author.accountId, label: value.author.label }, body,
    replyTo: value.replyTo === null ? null : messageId(value.replyTo), createdAt: value.createdAt, removedAt: value.removedAt, canRemove: value.canRemove };
}
function verifySend(pending, value) {
  check(object(value) && size(value) <= 256 * 1024 && value.requestId === pending.args.requestId && typeof value.replayed === 'boolean'
    && object(value.receipt) && value.receipt.conversationId === pending.scope.conversationId && integer(value.receipt.createdAt)
    && object(value.ownCurrent) && typeof value.ownCurrent.removed === 'boolean', 'app_discussion_invalid_receipt');
  const receipt = { id: messageId(value.receipt.id), conversationId: pending.scope.conversationId, createdAt: value.receipt.createdAt };
  const message = value.message === null ? null : messageValue(value.message, receipt.conversationId);
  if (message) check(message.id === receipt.id && message.createdAt === receipt.createdAt && message.author.accountId === pending.scope.accountId
    && (message.body === null ? value.ownCurrent.removed : message.body === pending.args.body && !value.ownCurrent.removed)
    && message.replyTo === (pending.args.replyTo ?? null), 'app_discussion_invalid_receipt');
  return { requestId: value.requestId, replayed: value.replayed, receipt, ownCurrent: { removed: value.ownCurrent.removed }, message };
}
export async function dispatchAppDiscussionIntent({ state, api, isCurrent, expectedDraft, beforeCreate, expectedPending }) {
  if (!isCurrent()) return { status: 'stale' };
  check(!(expectedDraft && expectedPending), 'app_discussion_invalid_pending');
  const pending = expectedDraft ? await state.prepareSend(expectedDraft, beforeCreate) : await state.pendingForDispatch(expectedPending);
  if (!isCurrent()) return { status: 'stale', pending };
  const response = verifySend(pending, await api.request('apps.discussion.send', clone(pending.args)));
  const accepted = await state.acknowledge(pending, response);
  if (!isCurrent()) return { status: 'stale', pending };
  return { status: accepted ? 'accepted' : 'superseded', pending, response };
}

function cursorValue(value) {
  check(value === null || typeof value === 'string' && /^[A-Za-z0-9_.-]{1,2048}$/u.test(value)); return value;
}
export function createAppDiscussionFeed({ accountId, appId, entry: rawEntry, administrative = false }) {
  check(id(accountId) && typeof appId === 'string' && APP.test(appId) && [true, false].includes(administrative), 'app_discussion_invalid_scope');
  const entry = rawEntry === null ? null : normalizeAppEntry(rawEntry);
  check(administrative ? entry === null : entry?.appId === appId, 'app_discussion_invalid_scope');
  const listeners = new Set(), active = new Map();
  let disposed = false, selectionGeneration = 0, messageGeneration = 0, archiveGeneration = 0;
  const empty = () => ({ context: null, messages: [], historyCursor: null, changeCursor: null, archives: [], nextArchiveCursor: null,
    loading: false, stale: false, resetRequired: false, hasNewer: false, window: 'latest', windowRevision: 0, error: null });
  let state = empty();
  const emit = () => { state.loading = active.size > 0; if (!disposed) for (const listener of listeners) listener(); };
  const args = () => ({ expectedAccountId: accountId, appId, ...(administrative ? { administrative: true } : { domainId: entry.domainId, path: entry.path }) });
  function contextValue(value, conversationId) {
    check(object(value) && value.appId === appId && typeof value.conversationId === 'string' && CONV.test(value.conversationId)
      && (!conversationId || value.conversationId === conversationId) && ['current', 'archive'].includes(value.mode)
      && typeof value.isCurrent === 'boolean' && value.ownerAdministrative === administrative && ['public', 'shared', 'owner'].includes(value.audience)
      && typeof value.canPost === 'boolean' && typeof value.canModerate === 'boolean'
      && (!value.canPost || value.isCurrent && value.mode === 'current' && !administrative));
    const resolved = value.entry === null ? null : normalizeAppEntry(value.entry);
    check(same(resolved, entry));
    return { appId, entry: resolved, conversationId: value.conversationId, mode: value.mode, isCurrent: value.isCurrent,
      ownerAdministrative: administrative, audience: value.audience, canPost: value.canPost, canModerate: value.canModerate };
  }
  const fits = messages => messages.length <= 500 && size(messages) <= 2 * 1024 * 1024;
  function merge(previous, incoming) {
    const prior = new Map(previous.map(row => [row.id, row]));
    return incoming.map(row => {
      const old = prior.get(row.id);
      if (old?.body === null && row.body !== null) return { ...row, body: null, removedAt: old.removedAt, canRemove: false,
        ...(old.removalConfirmed ? { removalConfirmed: true } : {}) };
      return row;
    });
  }
  function pageMessages(value, context, maximum) {
    check(Array.isArray(value) && value.length <= maximum);
    const rows = value.map(message => messageValue(message, context.conversationId));
    check(new Set(rows.map(row => row.id)).size === rows.length); return rows;
  }
  async function request(kind, { api, isCurrent }, action) {
    if (disposed || !isCurrent()) return 'stale';
    if (kind === 'load') { selectionGeneration++; messageGeneration++; archiveGeneration++; active.clear(); }
    const selection = selectionGeneration, version = kind === 'archives' ? ++archiveGeneration : ++messageGeneration;
    const slot = kind === 'archives' ? 'archives' : 'messages'; active.set(slot, version);
    const current = () => !disposed && isCurrent() && selectionGeneration === selection
      && (kind === 'archives' ? archiveGeneration : messageGeneration) === version;
    state.error = null; emit();
    const fetch = async (operation, params) => {
      const value = await api.request(`apps.discussion.${operation}`, params);
      if (!current()) return null;
      check(object(value) && size(value) <= 256 * 1024, 'app_discussion_invalid_page'); return value;
    };
    try { return await action(fetch, current); }
    catch (error) {
      if (!current()) return 'stale';
      const denied = ['ACTIVE_PROFILE_CHANGED', 'authentication_required', 'device_revoked', 'apps_authentication_required',
        'apps_account_changed', 'access_denied', 'app_unavailable'].includes(error?.code);
      if (denied) {
        // Archives and message pages can run independently. An affirmative
        // authority refusal invalidates both, including already fetched pages.
        selectionGeneration++; messageGeneration++; archiveGeneration++; active.clear(); state = empty();
      }
      state.stale = true; state.error = error?.code ?? 'app_discussion_load_failed';
      if (denied) emit();
      throw error;
    } finally { if (active.get(slot) === version) active.delete(slot); if (current()) emit(); }
  }
  function reset(context) {
    state = { ...state, context, messages: [], historyCursor: null, changeCursor: null, resetRequired: true, stale: false,
      hasNewer: false, windowRevision: state.windowRevision + 1 }; return 'reset';
  }
  const model = {
    read: () => clone(state), subscribe(listener) { listeners.add(listener); return () => listeners.delete(listener); },
    invalidate() { selectionGeneration++; active.clear(); state = empty(); emit(); },
    dispose() { disposed = true; selectionGeneration++; active.clear(); state = empty(); listeners.clear(); },
    async load(options) {
      const selected = options.conversationId;
      check(selected === undefined || typeof selected === 'string' && CONV.test(selected));
      return request('load', options, async (fetch, current) => {
        const nextWindow = state.windowRevision + 1; state = { ...empty(), windowRevision: nextWindow }; emit();
        const value = await fetch('context', { ...args(), ...(selected ? { conversationId: selected } : {}) });
        if (!value || !current()) return 'stale';
        const context = contextValue(value.context, selected), messages = pageMessages(value.messages, context, 30);
        check(fits(messages));
        state = { ...empty(), context, messages, historyCursor: cursorValue(value.historyCursor), changeCursor: cursorValue(value.changeCursor), windowRevision: nextWindow };
        check(state.changeCursor !== null); return 'accepted';
      });
    },
    async older(options) {
      if (!state.context || !state.historyCursor) return 'accepted';
      const selected = state.context.conversationId, cursor = state.historyCursor;
      return request('messages', options, async (fetch, current) => {
        const value = await fetch('history', { ...args(), conversationId: selected, cursor, limit: 50 });
        if (!value || !current()) return 'stale';
        const context = contextValue(value.context, selected); check(typeof value.resetRequired === 'boolean');
        if (value.resetRequired) return reset(context);
        const older = merge(state.messages, pageMessages(value.messages, context, 50));
        const ids = new Set(older.map(row => row.id)); const combined = [...older, ...state.messages.filter(row => !ids.has(row.id))];
        const windowChanged = !fits(combined); check(fits(older));
        state.context = context; state.messages = windowChanged ? older : combined; state.historyCursor = cursorValue(value.nextCursor);
        if (windowChanged) { state.window = 'history'; state.hasNewer = true; state.windowRevision++; }
        state.stale = false; state.resetRequired = false; return 'accepted';
      });
    },
    async poll(options) {
      if (!state.context || !state.changeCursor) return 'accepted';
      const selected = state.context.conversationId;
      return request('messages', options, async (fetch, current) => {
        for (let page = 0; page < 4; page++) {
          const previousCursor = state.changeCursor;
          const value = await fetch('changes', { ...args(), conversationId: selected, cursor: previousCursor, limit: 100 });
          if (!value || !current()) return 'stale';
          const context = contextValue(value.context, selected);
          check(typeof value.resetRequired === 'boolean' && typeof value.hasMore === 'boolean');
          if (value.resetRequired) return reset(context);
          check(Array.isArray(value.changes) && value.changes.length <= 100);
          let messages = [...state.messages];
          for (const event of value.changes) {
            check(object(event) && ['message', 'removed'].includes(event.type));
            const row = merge(messages, [messageValue(event.message, selected)])[0];
            check((event.type === 'removed') === (row.body === null));
            const at = messages.findIndex(item => item.id === row.id);
            if (at >= 0) messages[at] = row;
            else if (row.body !== null) {
              if (state.window === 'latest' && fits([...messages, row])) messages.push(row);
              else { state.hasNewer = true; state.window = 'history'; }
            }
          }
          state.context = context; state.messages = messages; state.changeCursor = cursorValue(value.nextCursor);
          check(state.changeCursor !== null && (!value.hasMore || value.changes.length > 0 && state.changeCursor !== previousCursor));
          state.stale = false; state.resetRequired = false;
          if (!value.hasMore) { if (state.window === 'latest') state.hasNewer = false; return 'accepted'; }
        }
        return 'accepted'; // bounded catch-up: the next poll resumes this fetched cursor
      });
    },
    async archives(options) {
      const cursor = options.older ? state.nextArchiveCursor : undefined;
      if (options.older && !cursor) return 'accepted';
      return request('archives', options, async (fetch, current) => {
        const value = await fetch('archives', { ...args(), limit: 20, ...(cursor ? { cursor } : {}) });
        if (!value || !current()) return 'stale';
        check(typeof value.resetRequired === 'boolean' && Array.isArray(value.entries) && value.entries.length <= 20);
        if (value.resetRequired) { state.archives = []; state.nextArchiveCursor = null; return 'reset'; }
        const entries = value.entries.map(item => contextValue(item));
        check(entries.every(item => item.mode === 'archive' && !item.canPost && !item.isCurrent)
          && new Set(entries.map(item => item.conversationId)).size === entries.length);
        state.archives = entries; state.nextArchiveCursor = cursorValue(value.nextCursor); return 'accepted';
      });
    },
    acceptSend(expected, response) {
      const pending = pendingValue(expected), value = verifySend(pending, response);
      if (disposed || pending.scope.accountId !== accountId || pending.scope.appId !== appId || !same(pending.scope.entry, entry)
        || state.context?.conversationId !== pending.scope.conversationId || !value.message) return false;
      messageGeneration++; active.delete('messages');
      const row = merge(state.messages, [value.message])[0], index = state.messages.findIndex(item => item.id === row.id);
      if (index >= 0) state.messages[index] = row;
      // An ACK may overtake unread messages. Only the fetched change stream
      // establishes order; the caller polls immediately after this receipt.
      else state.hasNewer = true;
      emit(); return true;
    },
    acceptRemoval(value) {
      check(object(value) && value.removed === true && typeof value.conversationId === 'string' && CONV.test(value.conversationId)); messageId(value.id);
      if (disposed || state.context?.conversationId !== value.conversationId) return false;
      messageGeneration++; active.delete('messages');
      state.messages = state.messages.map(row => row.id === value.id ? { ...row, body: null, canRemove: false, removalConfirmed: true } : row);
      emit(); return true;
    },
  };
  return model;
}
