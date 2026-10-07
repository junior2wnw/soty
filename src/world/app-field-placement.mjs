import { createFieldDocument, validateFieldDocument, validateFieldEntity, fieldEntityKey } from '../../modules/field/contract.mjs';
import { applyFieldCommand, nextFieldSlot } from './unified-field-state.mjs';

const APP_ID = /^app-[a-f0-9]{32}$/u;
const fail = code => { throw Object.assign(new Error(code), { code }); };
const clone = value => structuredClone(value);
const appRef = id => { if (typeof id !== 'string' || !APP_ID.test(id)) fail('app_field_invalid_app'); return { kind: 'app', id }; };
const initialApps = ids => {
  if (!Array.isArray(ids) || ids.length > 100) fail('app_field_invalid_initial_refs');
  return [...new Set(ids.map(id => appRef(id).id))];
};

/** Shared first-use layout; a reference never registers an app or grants access. */
export function createPersonalFieldDocument({ includeBuiltins = true, initialAppIds = [] } = {}) {
  const document = createFieldDocument();
  document.contexts.push({ contextId: 'personal', title: 'Личное', x: 0, y: 0 });
  if (includeBuiltins) for (const id of ['notes', 'chess']) {
    document.shortcuts.push({ shortcutId: `builtin-${id}`, entity: { kind: 'builtin', id }, contextId: 'personal',
      slot: nextFieldSlot(document, 'personal', 'builtin') });
  }
  for (const id of initialApps(initialAppIds)) document.shortcuts.push({ shortcutId: `migrated-${id}`, entity: appRef(id), contextId: 'personal',
    slot: nextFieldSlot(document, 'personal', 'app') });
  return validateFieldDocument(document);
}

/** Idempotent within one space; the same entity can be placed in another space. */
export function ensureFieldEntityPlacement(value, { entity, contextId, shortcutId, initializePersonal = false, includeBuiltins = true, initialAppIds = [] }) {
  let document = validateFieldDocument(value); const ref = validateFieldEntity(entity);
  let createdContext = false;
  if (!document.contexts.length && initializePersonal && contextId === 'personal') {
    document = createPersonalFieldDocument({ includeBuiltins, initialAppIds }); createdContext = true;
  }
  if (!document.contexts.some(context => context.contextId === contextId)) fail('field_context_missing');
  const existing = document.shortcuts.find(shortcut => shortcut.contextId === contextId && fieldEntityKey(shortcut.entity) === fieldEntityKey(ref));
  if (existing) return { document, changed: createdContext, shortcutId: existing.shortcutId, createdContext };
  const change = applyFieldCommand(document, { type: 'add-shortcut', shortcutId, entity: ref, contextId });
  return { document: change.document, changed: change.changed, shortcutId, createdContext };
}

/** The supplied port is the existing account-scoped field persistence authority.
 * No URL, admission, registry mutation or saved-entry operation is accepted. */
export function createAppFieldPlacementController({ persistence, accountId, appId, isCurrent = () => true, signal,
  randomId = () => crypto.randomUUID(), initialAppIds = [] }) {
  const entity = appRef(appId);
  const initialRefs = initialApps(initialAppIds);
  if (typeof accountId !== 'string' || !accountId || !persistence || typeof persistence.load !== 'function') fail('app_field_invalid_controller');
  let disposed = false, aborted = false, loaded = false, generation = 1, contextKey = '', queued = 0;
  let queue = Promise.resolve(), last = null;
  const listeners = new Set(), abortListeners = new Set();
  const check = () => {
    if (aborted || signal?.aborted) fail('app_field_aborted');
    if (disposed || !isCurrent()) fail('field_account_changed');
  };
  const contextSignature = context => JSON.stringify(context);
  function publish(state) {
    const contexts = state.document.contexts.length ? state.document.contexts : [{ contextId: 'personal', title: 'Личное', x: 0, y: 0 }];
    const nextKey = JSON.stringify([state.document.contexts.length === 0, contexts]);
    if (contextKey && contextKey !== nextKey) generation++;
    contextKey = nextKey;
    const placements = state.document.shortcuts.filter(shortcut => fieldEntityKey(shortcut.entity) === fieldEntityKey(entity))
      .map(shortcut => ({ contextId: shortcut.contextId, shortcutId: shortcut.shortcutId }));
    last = { appId, generation, revision: state.revision, projectedRevision: state.projectedRevision,
      persistence: state.state, localDurable: state.localDurable, pendingCount: state.pendingCount,
      contexts: contexts.map(context => ({ contextId: context.contextId, title: context.title,
        placed: placements.some(placement => placement.contextId === context.contextId), willCreate: state.document.contexts.length === 0 })),
      placements, ...(state.errorCode ? { errorCode: state.errorCode } : {}) };
    if (!disposed && isCurrent()) for (const listener of listeners) {
      try { listener(clone(last)); } catch { /* A view observer cannot alter persistence. */ }
    }
    return clone(last);
  }
  const unobserve = persistence.subscribe(state => { if (!disposed && isCurrent()) publish(state); });
  function dispose() {
    if (disposed) return; disposed = true; unobserve(); listeners.clear();
    for (const remove of abortListeners) remove(); abortListeners.clear(); persistence.dispose();
  }
  function bindAbort(value) {
    if (!value) return () => {};
    const end = () => { aborted = true; dispose(); };
    if (value.aborted) end(); else value.addEventListener('abort', end, { once: true });
    const remove = () => { value.removeEventListener('abort', end); abortListeners.delete(remove); };
    abortListeners.add(remove); return remove;
  }
  bindAbort(signal);
  function serialize(action, actionSignal) {
    check(); if (queued >= 4) fail('app_field_busy'); queued++;
    const removeAbort = bindAbort(actionSignal);
    const next = queue.catch(() => {}).then(async () => {
      check(); try { return await action(); } catch (error) { check(); throw error; }
    });
    const result = next.finally(() => { queued--; removeAbort(); });
    queue = result; return result;
  }
  async function read() {
    check();
    // A pending/undurable intent is settled by its original ID. A passive load
    // must not replace it with a newly computed placement.
    const prior = loaded ? persistence.getState() : null;
    const state = prior && (prior.pendingCount || prior.state === 'conflict' || persistence.hasUnsavedChanges())
      ? await persistence.retry() : await persistence.load();
    check(); loaded = true; publish(state); return state;
  }
  function status(state) {
    if (state.state === 'conflict') return 'conflict';
    if (persistence.hasUnsavedChanges() || !state.localDurable) return 'storage-error';
    if (state.pendingCount || ['saving', 'offline', 'storage-error'].includes(state.state)) return 'pending';
    return 'saved';
  }
  function result(state, changed, shortcutId, errorCode) {
    return { status: errorCode === 'app_field_selection_changed' ? 'conflict' : status(state), changed, shortcutId,
      snapshot: publish(state), ...(errorCode ? { errorCode } : {}) };
  }
  async function place({ contextId, generation: selectedGeneration, signal: actionSignal } = {}) {
    return serialize(async () => {
      if (!loaded) await read(); check();
      if (!Number.isSafeInteger(selectedGeneration) || selectedGeneration !== generation) fail('app_field_selection_changed');
      const previous = persistence.getState();
      const expectedContext = previous.document.contexts.find(context => context.contextId === contextId);
      const expectedEmpty = !previous.document.contexts.length && contextId === 'personal';
      if (!expectedContext && !expectedEmpty) fail('field_context_missing');
      const expectedKey = expectedContext ? contextSignature(expectedContext) : 'empty-personal';
      let state = await read(); check();
      const stableContext = () => {
        const context = state.document.contexts.find(value => value.contextId === contextId);
        return expectedEmpty ? !state.document.contexts.length : context && contextSignature(context) === expectedKey;
      };
      if (!stableContext()) return result(state, false, null, 'app_field_selection_changed');
      if (state.state === 'conflict' || state.pendingCount || persistence.hasUnsavedChanges() || !state.localDurable
        || state.errorCode === 'field_invalid_receipt') return result(state, false, null, state.errorCode);
      let shortcutId;
      for (let attempt = 0; attempt < 2; attempt++) {
        check();
        const existing = state.document.shortcuts.find(shortcut => shortcut.contextId === contextId && fieldEntityKey(shortcut.entity) === fieldEntityKey(entity));
        if (existing) return result(state, false, existing.shortcutId);
        shortcutId ??= randomId();
        const desired = ensureFieldEntityPlacement(state.document, { entity, contextId, shortcutId,
          initializePersonal: expectedEmpty, includeBuiltins: state.revision === 0, initialAppIds: state.revision === 0 ? initialRefs : [] });
        const requestId = randomId();
        let commit;
        try { commit = await persistence.commit(desired.document, { expectedRevision: state.revision, requestId }); }
        catch (error) {
          check(); state = persistence.getState();
          if (['field_local_storage_unavailable', 'field_local_revision_conflict', 'field_local_capacity'].includes(error?.code)) {
            return result(state, false, desired.shortcutId, error.code);
          }
          throw error;
        }
        check(); state = persistence.getState();
        if (commit.status !== 'conflict') return result(state, true, desired.shortcutId, commit.errorCode);
        const pending = persistence.exportPending().pending;
        // Only our fresh, explicitly rejected CAS can be rebased. Unknown ACK,
        // accepted-but-superseded result, other intents or shared-store races
        // stay held; they never authorize replacing the entire newer layout.
        if (attempt !== 0 || state.errorCode !== 'field_revision_conflict' || pending.length !== 1
          || pending[0].requestId !== requestId || persistence.hasUnsavedChanges()) return result(state, false, null, state.errorCode);
        state = await persistence.resolveConflict('remote'); check(); publish(state);
        if (!stableContext()) return result(state, false, null, 'app_field_selection_changed');
        if (status(state) !== 'saved') return result(state, false, null, state.errorCode);
      }
      return result(state, false, null, state.errorCode);
    }, actionSignal);
  }
  return Object.freeze({
    load: actionSignal => serialize(async () => { await read(); return clone(last); }, actionSignal),
    refresh: actionSignal => serialize(async () => { await read(); return clone(last); }, actionSignal),
    place,
    retry: actionSignal => serialize(async () => { const state = loaded ? await persistence.retry() : await read(); check(); loaded = true; return publish(state); }, actionSignal),
    getSnapshot: () => { check(); return last ? clone(last) : null; },
    subscribe(listener) { check(); listeners.add(listener); return () => listeners.delete(listener); },
    hasUnsavedChanges: () => !disposed && loaded && persistence.hasUnsavedChanges(),
    flush: () => serialize(async () => { if (!loaded) await read(); const state = await persistence.flush(); check(); return publish(state); }),
    dispose,
  });
}
