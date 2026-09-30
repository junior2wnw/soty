const id = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{3,180}$/u.test(value);
const clone = value => structuredClone(value);
const fail = code => { throw Object.assign(new Error(code), { code }); };
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const fields = ['text', 'cwd', 'hostDeviceId', 'connectorId'];
const freshDraft = () => ({ text: '', cwd: '', hostDeviceId: '', connectorId: '' });
function draft(value) {
  if (!value || typeof value !== 'object' || fields.some(key => typeof value[key] !== 'string')) fail('app_create_storage_unavailable');
  if (value.text.length > 16_000 || value.cwd.length > 2000 || value.hostDeviceId.length > 180 || value.connectorId.length > 180) fail('app_create_storage_unavailable');
  return Object.fromEntries(fields.map(key => [key, value[key]]));
}
function target(value) {
  if (!value || !id(value.hostDeviceId) || !id(value.connectorId) || !id(value.jobId)) fail('invalid_app_create_receipt');
  return { hostDeviceId: value.hostDeviceId, connectorId: value.connectorId, jobId: value.jobId };
}
function payload(value, withRequest = false) {
  if (!value || !id(value.expectedAccountId) || !id(value.hostDeviceId) || !id(value.connectorId)) fail('invalid_app_create_target');
  if (typeof value.text !== 'string' || !value.text.trim() || value.text.length > 16_000 || typeof value.cwd !== 'string'
    || value.cwd.length > 2000 || /[\u0000-\u001f]/u.test(value.cwd)) fail('invalid_app_create_prompt');
  if (withRequest && !id(value.requestId)) fail('invalid_app_create_receipt');
  return { expectedAccountId: value.expectedAccountId, hostDeviceId: value.hostDeviceId, connectorId: value.connectorId,
    text: value.text.trim(), cwd: value.cwd.trim(), ...(withRequest ? { requestId: value.requestId } : {}) };
}

/** One account has one recoverable app-creation intent. All read/modify/write
 * transitions use the same Web Lock. No network await holds that lock. */
export function createAppCreateState({ accountId, storage, locks, randomId = () => crypto.randomUUID() }) {
  if (!id(accountId)) fail('invalid_account');
  const key = `soty.app-create.v1:${accountId}`;
  const fresh = () => ({ schema: 1, accountId, draft: freshDraft(), revision: 0, pending: null, accepted: null });
  let last = fresh(), volatileDraft = null, readFailed = false, draftGeneration = 0;
  function load() {
    try {
      const raw = storage.getItem(key);
      if (raw === null) { last = fresh(); readFailed = false; return last; }
      if (typeof raw !== 'string' || raw.length > 220_000) fail('app_create_storage_unavailable');
      const value = JSON.parse(raw);
      if (value?.schema !== 1 || value.accountId !== accountId || !Number.isSafeInteger(value.revision) || value.revision < 0) fail('app_create_storage_unavailable');
      const next = { schema: 1, accountId, draft: draft(value.draft), revision: value.revision, pending: null, accepted: value.accepted ? target(value.accepted) : null };
      if (value.pending) {
        const request = payload(value.pending.payload, true);
        if (request.expectedAccountId !== accountId || !Number.isSafeInteger(value.pending.draftRevision)
          || value.pending.draftRevision < 0 || value.pending.draftRevision > next.revision || next.accepted) fail('app_create_storage_unavailable');
        next.pending = { payload: request, draftRevision: value.pending.draftRevision };
      }
      last = next; readFailed = false; return next;
    } catch { readFailed = true; fail('app_create_storage_unavailable'); }
  }
  function write(next) {
    try { storage.setItem(key, JSON.stringify(next)); last = next; }
    catch { fail('app_create_storage_unavailable'); }
  }
  const lock = (action, required = false) => {
    if (locks?.request) return locks.request(key, action);
    if (required) return Promise.reject(Object.assign(new Error('app_create_lock_unavailable'), { code: 'app_create_lock_unavailable' }));
    return Promise.resolve().then(action);
  };
  function read() {
    try { load(); } catch { /* Keep the last visible draft; dispatch is fail-closed. */ }
    return clone({ ...last, ...(volatileDraft ? { draft: volatileDraft } : {}) });
  }
  const mergeVolatile = value => volatileDraft ? { ...value, draft: clone(volatileDraft), revision: value.revision + 1 } : value;
  function matches(expected, actual) {
    return actual && same(payload(expected, true), actual.payload);
  }
  function stageDraft(patch) {
    // Must happen in the input event, before identity/storage awaits. Guards can
    // already see the user's exact text and an older delayed save cannot win.
    volatileDraft = draft({ ...read().draft, ...patch });
    return ++draftGeneration;
  }
  function persistDraft(generation) {
    return lock(() => {
      if (generation !== draftGeneration) return read();
      const next = mergeVolatile(load());
      write(next); volatileDraft = null; return clone(next);
    });
  }
  return {
    key, read, stageDraft, persistDraft,
    canDispatch: () => Boolean(locks?.request),
    hasUnsavedChanges: () => Boolean(volatileDraft) || readFailed,
    hasVolatileDraft: () => Boolean(volatileDraft),
    discardLocalDraft() { volatileDraft = null; draftGeneration++; },
    async saveDraft(patch) { return persistDraft(stageDraft(patch)); },
    async flush() {
      return lock(() => { const next = mergeVolatile(load()); if (volatileDraft) { write(next); volatileDraft = null; } });
    },
    async prepare(input) {
      const value = payload(input);
      if (value.expectedAccountId !== accountId) fail('invalid_account');
      return lock(() => {
        const next = mergeVolatile(load());
        if (next.accepted) fail('app_create_result_pending');
        if (next.pending) {
          const { requestId, ...prior } = next.pending.payload;
          if (!same(value, prior)) fail('app_create_pending_unconfirmed');
        } else {
          // Tie receipt clearing to this draft revision, not only equal text.
          const exact = draft({ text: input.text, cwd: input.cwd, hostDeviceId: input.hostDeviceId, connectorId: input.connectorId });
          if (!same(next.draft, exact)) { next.draft = exact; next.revision++; }
          const requestId = randomId(); if (!id(requestId)) fail('invalid_app_create_request');
          next.pending = { payload: { ...value, requestId }, draftRevision: next.revision };
        }
        write(next); volatileDraft = null; return clone(next.pending);
      }, true);
    },
    async acknowledge(expected, job) {
      return lock(() => {
        const next = mergeVolatile(load());
        if (!matches(expected, next.pending)) return false;
        const accepted = target(job), pending = next.pending;
        if (accepted.hostDeviceId !== pending.payload.hostDeviceId || accepted.connectorId !== pending.payload.connectorId) fail('invalid_app_create_receipt');
        if (next.revision === pending.draftRevision) { next.draft = { ...next.draft, text: '' }; next.revision++; }
        next.accepted = accepted; next.pending = null;
        write(next); volatileDraft = null; return true;
      }, true);
    },
    async reject(expected, receipt) {
      return lock(() => {
        const next = mergeVolatile(load());
        if (!matches(expected, next.pending)) return false;
        if (receipt?.status !== 'rejected' || receipt.requestId !== next.pending.payload.requestId
          || typeof receipt.reason !== 'string' || !/^[a-z][a-z0-9_]{2,100}$/u.test(receipt.reason)) fail('invalid_app_create_receipt');
        next.pending = null; write(next); volatileDraft = null; return true;
      }, true);
    },
    async adoptAccepted(job) {
      const accepted = target(job);
      return lock(() => {
        const next = mergeVolatile(load());
        if (next.pending || next.accepted) return false;
        next.accepted = accepted; write(next); volatileDraft = null; return true;
      }, true);
    },
    async clearAccepted(job) {
      const expected = target(job);
      return lock(() => {
        const next = mergeVolatile(load());
        if (next.pending || !same(expected, next.accepted)) return false;
        next.accepted = null; write(next); volatileDraft = null; return true;
      }, true);
    },
  };
}
