// A draft is a separate branch. Two tabs never silently replace each other's text.
export const NOTE_COLORS = Object.freeze(['plain', 'honey', 'sage', 'lilac', 'blue', 'coral']);
export const uid = () => crypto.randomUUID();
export const blankNote = () => ({ noteId: uid(), title: '', body: '', items: [], color: 'plain', pinned: false, state: 'active', revision: 0, createdAt: Date.now(), updatedAt: Date.now(), preview: '' });
const clone = value => structuredClone(value);
const documentOf = note => ({ title: note.title, body: note.body, items: clone(note.items), color: note.color, pinned: note.pinned, state: note.state });
export function noteErrorCode(error) { return String(error?.code || error?.error?.code || error?.message || 'notes_network_error'); }

/** Local storage is best effort and origin-scoped. Every key additionally includes project and verified account. */
export function createDraftStore(accountId, projectId = 'default') {
  const scope = JSON.stringify([location.origin, projectId, accountId]);
  let opening;
  const database = () => {
    if (!opening) opening = new Promise((resolve, reject) => {
      if (!globalThis.indexedDB) { reject(new Error('notes_local_unavailable')); return; }
      const request = indexedDB.open('soty-notes-drafts-v1', 1);
      request.onupgradeneeded = () => { const store = request.result.createObjectStore('drafts', { keyPath: 'key' }); store.createIndex('scope', 'scope'); };
      request.onerror = () => reject(new Error('notes_local_unavailable'));
      request.onblocked = () => reject(new Error('notes_local_unavailable'));
      request.onsuccess = () => { const db = request.result; db.onversionchange = () => { db.close(); opening = undefined; }; resolve(db); };
    }).catch(error => { opening = undefined; throw error; });
    return opening;
  };
  const keyOf = (noteId, branchId) => JSON.stringify([scope, noteId, branchId]);
  return {
    async put(value) {
      const record = { ...clone(value), scope, key: keyOf(value.note.noteId, value.branchId) };
      const bytes = new TextEncoder().encode(JSON.stringify(record)).length;
      if (bytes > 800 * 1024) throw new Error('notes_local_quota');
      const db = await database();
      return new Promise((resolve, reject) => {
        const tx = db.transaction('drafts', 'readwrite'); const store = tx.objectStore('drafts'); let failure;
        const records = store.index('scope').getAll(scope, 33);
        records.onsuccess = () => {
          const others = records.result.filter(item => item.key !== record.key);
          if (others.length >= 32 || others.reduce((sum, item) => sum + (item.bytes || 0), bytes) > 4 * 1024 * 1024) { failure = new Error('notes_local_quota'); tx.abort(); return; }
          store.put({ ...record, bytes });
        };
        tx.oncomplete = () => resolve();
        tx.onabort = tx.onerror = () => reject(failure || new Error('notes_local_unavailable'));
      });
    },
    async list() {
      const db = await database();
      return new Promise((resolve, reject) => {
        const tx = db.transaction('drafts', 'readonly'); const request = tx.objectStore('drafts').index('scope').getAll(scope, 33); let result;
        request.onsuccess = () => { result = request.result.sort((a, b) => b.savedAt - a.savedAt); };
        tx.oncomplete = () => resolve(result); tx.onabort = tx.onerror = () => reject(new Error('notes_local_unavailable'));
      });
    },
    async remove(noteId, branchId, savedAt) {
      const db = await database();
      return new Promise((resolve, reject) => {
        const tx = db.transaction('drafts', 'readwrite'); const store = tx.objectStore('drafts'); const key = keyOf(noteId, branchId); const request = store.get(key);
        // Conditional removal cannot delete a source branch that another tab changed meanwhile.
        request.onsuccess = () => { if (request.result && (savedAt === undefined || request.result.savedAt === savedAt)) store.delete(key); };
        tx.oncomplete = () => resolve(); tx.onabort = tx.onerror = () => reject(new Error('notes_local_unavailable'));
      });
    },
  };
}

/** Autosave is a durable local outbox followed by a serialized compare-and-swap.
 * A lost response retries the identical mutation, including after reload. */
export function createNoteSession({ api, accountId, store, note, draft, onChange = () => {}, debounceMs = 650 }) {
  let current = clone(draft?.note || note); const branchId = uid();
  let generation = draft ? draft.generation : 0;
  let committedGeneration = draft ? draft.committedGeneration : 0;
  let durableGeneration = draft ? generation : 0;
  let pending = draft?.pending ? clone(draft.pending) : null;
  let localQueue = Promise.resolve(); let sending = null; let retryFlight = null; let timer; let disposed = false; let abandoned = false;
  let conflict = Boolean(draft?.conflict); let error = ''; let localError = ''; let lastSavedAt = 0;
  let source = draft ? { noteId: draft.note.noteId, branchId: draft.branchId, savedAt: draft.savedAt } : null;
  const notify = () => { if (!disposed) onChange(state()); };
  const state = () => ({ note: clone(current), dirty: generation > committedGeneration || Boolean(pending), localDurable: durableGeneration >= generation,
    saving: Boolean(sending), conflict, error, localError, branchId });
  function snapshot() { lastSavedAt = Math.max(Date.now(), lastSavedAt + 1); return { note: clone(current), branchId, generation, committedGeneration,
    pending: pending ? clone(pending) : null, conflict, savedAt: lastSavedAt }; }
  function persist() {
    const record = snapshot();
    // Persist the exact captured generation. A later write may already be waiting behind it.
    localQueue = localQueue.catch(() => {}).then(async () => {
      try {
        await store.put(record); durableGeneration = Math.max(durableGeneration, record.generation); localError = '';
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
  function schedule() { clearTimeout(timer); if (!disposed) timer = setTimeout(() => { void save(); }, debounceMs); }
  async function save() {
    clearTimeout(timer);
    if (sending) return sending;
    if (disposed || abandoned || conflict || (generation === committedGeneration && !pending)) return;
    sending = (async () => {
      try {
        while (!conflict && (pending || generation > committedGeneration)) {
          if (!pending) pending = { mutationId: uid(), expectedRevision: current.revision, generation, document: documentOf(current) };
          // Do not send an outbox that cannot survive a lost HTTP response. This also guarantees recoverable offline text.
          await persist();
          if (disposed || abandoned) break;
          const outbox = clone(pending);
          const ack = await api.request('notes.put', { expectedAccountId: accountId, noteId: current.noteId,
            mutationId: outbox.mutationId, expectedRevision: outbox.expectedRevision, ...outbox.document });
          current.revision = ack.revision; current.updatedAt = ack.updatedAt;
          current.preview = (current.body.trim() || current.items.map(item => `${item.done ? '✓ ' : '□ '}${item.text}`).join(' · ')).replace(/\s+/gu, ' ').slice(0, 180);
          committedGeneration = outbox.generation; pending = null; error = '';
          await persist(); await clean();
          if (disposed) break;
        }
      } catch (cause) {
        error = noteErrorCode(cause);
        if (['notes_revision_conflict', 'notes_note_deleted', 'notes_note_not_found'].includes(error)) { conflict = true; await persist().catch(() => {}); }
      } finally { sending = null; notify(); }
    })();
    notify(); return sending;
  }
  function retry() {
    if (disposed || abandoned) return Promise.resolve();
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
  if (draft) { void persist().catch(() => {}); if (!conflict) schedule(); }
  return {
    state,
    edit(patch) {
      current = { ...current, ...clone(patch) }; generation++; error = ''; void persist().catch(() => {}); schedule(); notify();
    },
    retry,
    async flush() {
      clearTimeout(timer); if (generation > committedGeneration || pending) await persist();
      await localQueue; await save(); await localQueue;
      if (durableGeneration < generation || localError) throw new Error(localError || 'notes_local_unavailable');
    },
    hasUnsavedChanges() { return durableGeneration < generation; },
    async discardBranch() {
      clearTimeout(timer); await localQueue.catch(() => {}); if (sending) await sending;
      await store.remove(current.noteId, branchId);
      if (source) await store.remove(source.noteId, source.branchId, source.savedAt);
      source = null; abandoned = true;
    },
    dispose() { clearTimeout(timer); disposed = true; if (!abandoned && (generation > committedGeneration || pending)) void persist().catch(() => {}); },
  };
}
