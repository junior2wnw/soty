import { invalidateNoteCache } from './cache.mjs';
import {createNativeDraftStore} from './native-local.mjs';

// A draft is a separate branch. Two tabs never silently replace each other's text.
export const NOTE_COLORS = Object.freeze(['plain', 'honey', 'sage', 'lilac', 'blue', 'coral']);
export const uid = () => crypto.randomUUID();
export const blankNote = () => ({ noteId: uid(), title: '', body: '', items: [], color: 'plain', pinned: false, state: 'active', revision: 0, createdAt: Date.now(), updatedAt: Date.now(), preview: '' });
const clone = value => structuredClone(value);
const documentOf = note => ({ title: note.title, body: note.body, items: clone(note.items), color: note.color, pinned: note.pinned, state: note.state });
export function noteErrorCode(error) { return String(error?.code || error?.error?.code || error?.message || 'notes_network_error'); }

/** Local storage is best effort and origin-scoped. Every key additionally includes project and verified account. */
export function createDraftStore(accountId, projectId = 'default', options) { return createNativeDraftStore(accountId, projectId, options); }

/** Autosave is a durable local outbox followed by a serialized compare-and-swap.
 * A lost response retries the identical mutation, including after reload. */
export function createNoteSession({ api, accountId, store, note, draft, cache, onChange = () => {}, debounceMs = 650 }) {
  let current = clone(draft?.note || note); const branchId = uid();
  let generation = draft ? draft.generation : 0;
  let committedGeneration = draft ? draft.committedGeneration : 0;
  let durableGeneration = draft ? generation : 0;
  let pending = draft?.pending ? clone(draft.pending) : null;
  // Kept inside the same durable draft until a successor ACK is committed.
  // A restored branch has a new branchId; mutation identity must survive it.
  const retiredMutations = clone(draft?.retiredMutations || []);
  let localQueue = Promise.resolve(); let sending = null; let retryFlight = null; let timer; let disposed = false; let abandoned = false;
  let resolutionFlight = null;
  let conflict = Boolean(draft?.conflict); let error = ''; let localError = ''; let cacheError = ''; let confirmedAt = 0; let lastSavedAt = 0;
  let suspended = false, localProtection = null, unregister = null;
  let source = draft ? { noteId: draft.note.noteId, branchId: draft.branchId, savedAt: draft.savedAt } : null;
  const notify = () => { if (!disposed) onChange(state()); };
  const state = () => ({ note: localProtection ? { ...clone(current), title: '', body: '', preview: '', items: [] } : clone(current), dirty: generation > committedGeneration || Boolean(pending), localDurable: durableGeneration >= generation,
    saving: Boolean(sending), conflict, error, localError, cacheError, confirmedAt, branchId, ...(localProtection ? { localProtection } : {}) });
  const retirementSnapshot = () => retiredMutations.length ? { retiredMutations: clone(retiredMutations) } : {};
  const retireMutation = (mutationId, atGeneration) => { if (mutationId && !retiredMutations.some(value => value.mutationId === mutationId)) retiredMutations.push({mutationId, generation:atGeneration}); };
  const privateSnapshot = () => ({ note: clone(current), branchId, generation, committedGeneration, pending: pending ? clone(pending) : null, conflict, savedAt: lastSavedAt || Date.now(), ...retirementSnapshot() });
  function snapshot() { lastSavedAt = Math.max(Date.now(), lastSavedAt + 1); return { note: clone(current), branchId, generation, committedGeneration,
    pending: pending ? clone(pending) : null, conflict, savedAt: lastSavedAt, ...retirementSnapshot() }; }
  function persist() {
    const record = snapshot();
    // Persist the exact captured generation. A later write may already be waiting behind it.
    localQueue = localQueue.catch(() => {}).then(async () => {
      try {
        await store.put(record); durableGeneration = Math.max(durableGeneration, record.generation); localError = '';
        const retired = new Set((record.retiredMutations || []).filter(value => value.generation <= record.committedGeneration).map(value => value.mutationId));
        for (let index = retiredMutations.length - 1; index >= 0; index--) if (retired.has(retiredMutations[index].mutationId)) retiredMutations.splice(index, 1);
        if (source) {
          // Move the recovered branch only after its replacement is durable. If an older
          // tab wrote again, conditional removal leaves that independent branch intact.
          await store.remove(source.noteId, source.branchId, source.savedAt).catch(() => {}); source = null;
        }
      }
      catch (cause) { localError = noteErrorCode(cause); throw cause; }
      finally { notify(); }
    });
    // Failures remain observable through flush/state, while fire-and-forget typing never creates an unhandled rejection.
    localQueue.catch(() => {}); return localQueue;
  }
  async function clean() {
    if (generation !== committedGeneration || pending) return;
    try { await store.remove(current.noteId, branchId, lastSavedAt); if (source) { await store.remove(source.noteId, source.branchId, source.savedAt); source = null; } }
    catch { /* Keeping an already committed branch is safe; replay/CAS protects a subsequent recovery. */ }
  }
  function schedule() { clearTimeout(timer); if (!disposed && !suspended) timer = setTimeout(() => { void save(); }, debounceMs); }
  async function save(allowSuspended=false) {
    clearTimeout(timer);
    if (sending) return sending;
    if (disposed || abandoned || suspended&&!allowSuspended || conflict || (generation === committedGeneration && !pending)) return;
    sending = (async () => {
      try {
        while (!conflict && (pending || generation > committedGeneration)) {
          if (!pending) pending = { mutationId: uid(), expectedRevision: current.revision, generation, document: documentOf(current) };
          // Do not send an outbox that cannot survive a lost HTTP response. This also guarantees recoverable offline text.
          await persist();
          if (disposed || abandoned || suspended&&!allowSuspended) break;
          const outbox = clone(pending);
          let cacheToken = null;
          if (cache) { try { cacheToken = await cache.capture(); } catch { cacheError = 'notes_cache_unavailable'; } }
          if (disposed || abandoned || suspended&&!allowSuspended) break;
          const ack = await api.request('notes.put', { expectedAccountId: accountId, noteId: current.noteId,
            mutationId: outbox.mutationId, expectedRevision: outbox.expectedRevision, ...outbox.document });
          current.revision = ack.revision; current.updatedAt = ack.updatedAt;
          if (outbox.expectedRevision === 0) current.createdAt = ack.updatedAt;
          current.preview = (current.body.trim() || current.items.map(item => `${item.done ? '✓ ' : '□ '}${item.text}`).join(' · ')).replace(/\s+/gu, ' ').slice(0, 180);
          committedGeneration = outbox.generation; retireMutation(outbox.mutationId, outbox.generation); pending = null; error = ''; confirmedAt = Date.now();
          if (abandoned) break;
          if (cache) {
            // An ACK confirms the captured outbox, never the newer text typed
            // while this request was running. Cache failure cannot undo the ACK.
            const confirmed = { ...clone(current), ...clone(outbox.document) };
            confirmed.preview = (confirmed.body.trim() || confirmed.items.map(item => `${item.done ? '✓ ' : '□ '}${item.text}`).join(' · ')).replace(/\s+/gu, ' ').slice(0, 180);
            try { cacheError = await cache.remember(confirmed, cacheToken, confirmedAt) ? '' : 'notes_cache_unavailable'; }
            catch { cacheError = 'notes_cache_unavailable'; }
          }
          await persist(); await clean();
          if (disposed || suspended) break;
        }
      } catch (cause) {
        error = noteErrorCode(cause);
        if (cache) await invalidateNoteCache(cache, cause, current.noteId);
        if (['notes_revision_conflict', 'notes_note_deleted', 'notes_note_not_found'].includes(error)) { conflict = true; await persist().catch(() => {}); }
      } finally { sending = null; notify(); }
    })();
    notify(); return sending;
  }
  function retry() {
    if (disposed || abandoned || suspended) return Promise.resolve();
    if (retryFlight) return retryFlight;
    error = '';
    if (!sending) return save();
    // A healthy backend can be observed before an older request times out. Wait for
    // that result, then retry the durable outbox once; concurrent signals share it.
    const previous = sending;
    retryFlight = (async () => {
      await previous;
      if (!disposed && !abandoned && !conflict && (pending || generation > committedGeneration)) {
        error = ''; await save();
      }
    })().finally(() => { retryFlight = null; });
    return retryFlight;
  }
  function keepLocal() {
    if (disposed || abandoned || suspended || !conflict) return Promise.resolve();
    if (resolutionFlight) return resolutionFlight;
    const chosenGeneration = generation;
    resolutionFlight = (async () => {
      try {
        await flushLocal();
        if (disposed || abandoned || suspended || !conflict) return;
        const {note:remote} = await api.request('notes.get', {expectedAccountId:accountId, noteId:current.noteId});
        if (disposed || abandoned || suspended || !conflict) return;
        if (generation !== chosenGeneration) throw new Error('notes_local_draft_changed');
        if (remote?.noteId !== current.noteId || !Number.isSafeInteger(remote.revision) || remote.revision < current.revision) throw new Error('notes_revision_conflict');
        if (remote.state === 'trashed') throw new Error('notes_note_deleted');
        generation++;
        retireMutation(pending?.mutationId, generation);
        current = {...current, revision:remote.revision, createdAt:remote.createdAt, updatedAt:remote.updatedAt};
        pending = {mutationId:uid(), expectedRevision:remote.revision, generation, document:documentOf(current)};
        conflict = false; error = '';
        // Never overwrite the old outbox until this new explicit intent is durable.
        await persist(); await save();
      } catch (cause) { conflict = true; error = noteErrorCode(cause); }
      finally { notify(); }
    })().finally(() => { resolutionFlight = null; });
    return resolutionFlight;
  }
  async function flushLocal() {
    if(abandoned)return;
    clearTimeout(timer); if (generation > committedGeneration || pending) await persist();
    await localQueue;
    if (durableGeneration < generation || localError) throw new Error(localError || 'notes_local_unavailable');
    if(disposed){unregister?.();unregister=null;}
  }
  async function flush(allowSuspended=false) {
    if(abandoned)return;
    await flushLocal(); await save(allowSuspended); await localQueue;
    if (durableGeneration < generation || localError) throw new Error(localError || 'notes_local_unavailable');
  }
  const suspend = () => { suspended = true; localProtection = localProtection || 'pending'; clearTimeout(timer); notify(); };
  const sourcePort = { suspend, flush:()=>flush(true), flushLocal, snapshot: privateSnapshot,
    isDisposed:()=>disposed||abandoned,
    hasUnsavedChanges:()=>durableGeneration<generation||Boolean(localError),
    onGate(value) {
      if (value) { localProtection = value; suspend(); if(value==='pending')void flushLocal().catch(() => {}); }
      else { localProtection = null; suspended = false; if (!disposed && !conflict && (generation > committedGeneration || pending)) schedule(); notify(); }
    },
    releaseRecovered(expectedGeneration) {
      if (expectedGeneration !== generation) throw new Error('notes_local_draft_changed');
      abandoned = true; suspended = true; localProtection = 'sealed';durableGeneration=generation;localError=''; clearTimeout(timer); unregister?.(); unregister = null; notify();
    },
  };
  if (store.native?.register) unregister = store.native.register(current.noteId, sourcePort);
  if (draft) { void persist().catch(() => {}); if (!conflict) schedule(); }
  return {
    state,
    edit(patch) {
      if (disposed || abandoned || suspended) return;
      current = { ...current, ...clone(patch) }; generation++; error = ''; void persist().catch(() => {}); schedule(); notify();
    },
    retry, keepLocal,
    flush:()=>flush(), flushLocal,
    hasUnsavedChanges() { return durableGeneration < generation; },
    async discardBranch() {
      clearTimeout(timer); await localQueue.catch(() => {}); if (sending) await sending;
      await store.remove(current.noteId, branchId);
      if (source) await store.remove(source.noteId, source.branchId, source.savedAt);
      source = null; abandoned = true;
    },
    dispose() {
      clearTimeout(timer); disposed = true;
      const drained = !abandoned && (generation > committedGeneration || pending) ? persist() : localQueue;
      // A failed local commit retains this private registry snapshot for explicit
      // encrypted recovery; it never reappears through the ordinary loader.
      void drained.then(() => { unregister?.(); unregister = null; }, () => {});
    },
  };
}
