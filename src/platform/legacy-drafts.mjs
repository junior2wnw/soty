/** A Map-like draft store scoped to the local device identity. */
export function createLegacyDraftStore(storage, scope) {
  const memory = new Map(), dirty = new Set();
  // Deliberately outside the legacy `soty:` snapshot prefix: an unsubmitted
  // local draft must not silently enter room/account transfer snapshots.
  const keyFor = id => `soty.legacy-draft.v1:${encodeURIComponent(scope() || 'uninitialized')}:${encodeURIComponent(id)}`;
  function persist(key) {
    try {
      const value = memory.get(key);
      if (value === undefined) storage.removeItem(key); else storage.setItem(key, value);
      dirty.delete(key); return true;
    } catch { dirty.add(key); return false; }
  }
  const store = {
    get(id) {
      const key = keyFor(id);
      if (!memory.has(key)) {
        try { const value = storage.getItem(key); memory.set(key, typeof value === 'string' ? value : undefined); }
        catch { return undefined; }
      }
      return memory.get(key);
    },
    set(id, value) {
      const key = keyFor(id); memory.set(key, String(value)); persist(key); return store;
    },
    delete(id) {
      const had = store.get(id) !== undefined, key = keyFor(id);
      // A failed deletion keeps a tombstone in memory instead of reviving stale text.
      memory.set(key, undefined); persist(key); return had;
    },
    flush() { for (const key of [...dirty]) persist(key); return dirty.size === 0; },
    hasUnsavedChanges() { return dirty.size > 0; },
  };
  return store;
}
