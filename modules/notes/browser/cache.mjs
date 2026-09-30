// Only acknowledged, own-account snapshots belong here. Durable drafts/outbox use
// another database and are never candidates for cache eviction.
export const NOTE_CACHE_LIMITS = Object.freeze({ scopeCount: 20, scopeBytes: 2 * 1024 * 1024, count: 60, bytes: 6 * 1024 * 1024, entryBytes: 320 * 1024 });
const copy = value => structuredClone(value);
const codeOf = error => String(error?.code || error?.error?.code || error?.message || '').toLowerCase();
const failure = () => new Error('notes_cache_unavailable');
const noteKey = (scope, noteId) => JSON.stringify([scope, noteId]);
const byteSize = value => new TextEncoder().encode(JSON.stringify(value)).length;
const id = value => typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9_-]{7,95}$/u.test(value);

export function noteCacheErrorPolicy(error) {
  const code = codeOf(error);
  if (['notes_note_deleted', 'notes_note_not_found'].includes(code)) return 'missing';
  if (['notes_account_changed', 'authentication_required', 'device_revoked', 'account_revoked', 'active_profile_changed', 'no_local_profile',
    'account_mismatch', 'bootstrap_required', 'access_denied', 'forbidden', 'unauthorized', 'invalid_signature'].includes(code)
    || [401, 403].includes(error?.status)) return 'authorization';
  // Connect wraps fetch failures and timeouts. Parse errors, server responses,
  // revocations and arbitrary exceptions must never become an offline success.
  if (error?.status === undefined && ['network_error', 'network_timeout', 'notes_network_error'].includes(code)) return 'network';
  return 'other';
}

function validatedNote(value) {
  if (!value || !id(value.noteId) || !Number.isSafeInteger(value.revision) || value.revision < 1
    || !['active', 'archived', 'trashed'].includes(value.state) || !['plain', 'honey', 'sage', 'lilac', 'blue', 'coral'].includes(value.color)
    || typeof value.pinned !== 'boolean' || typeof value.title !== 'string' || value.title.length > 160
    || typeof value.body !== 'string' || value.body.length > 100000 || typeof value.preview !== 'string' || value.preview.length > 180
    || ![value.createdAt, value.updatedAt].every(time => Number.isSafeInteger(time) && time >= 0)
    || !Array.isArray(value.items) || value.items.length > 200) throw new Error('notes_cache_invalid_snapshot');
  const seen = new Set();
  const items = value.items.map(item => {
    if (!item || !id(item.id) || seen.has(item.id) || typeof item.text !== 'string' || item.text.length > 1000 || typeof item.done !== 'boolean') throw new Error('notes_cache_invalid_snapshot');
    seen.add(item.id); return { id: item.id, text: item.text, done: item.done };
  });
  return { noteId: value.noteId, title: value.title, body: value.body, items, preview: value.preview, state: value.state, color: value.color,
    pinned: value.pinned, revision: value.revision, createdAt: value.createdAt, updatedAt: value.updatedAt };
}

/** Pure admission/eviction policy; the driver executes it inside one IDB write transaction. */
export function planNoteCacheWrite(records, candidate, limits = NOTE_CACHE_LIMITS) {
  const existing = records.find(record => record.key === candidate.key);
  if (existing && existing.note.revision > candidate.note.revision) return { accepted: false, remove: [] };
  if (existing && existing.note.revision === candidate.note.revision && JSON.stringify(existing.note) !== JSON.stringify(candidate.note)) return { accepted: false, remove: [] };
  if (candidate.bytes > Math.min(limits.entryBytes, limits.scopeBytes, limits.bytes)) return { accepted: false, remove: [] };
  const others = records.filter(record => record.key !== candidate.key).sort((a, b) => a.usedAt - b.usedAt || a.key.localeCompare(b.key));
  const remove = [];
  const scopeEntries = () => others.filter(record => record.scope === candidate.scope);
  while (scopeEntries().length + 1 > limits.scopeCount || scopeEntries().reduce((sum, record) => sum + record.bytes, candidate.bytes) > limits.scopeBytes) {
    const index = others.findIndex(record => record.scope === candidate.scope); if (index < 0) return { accepted: false, remove: [] };
    remove.push(others.splice(index, 1)[0].key);
  }
  while (others.length + 1 > limits.count || others.reduce((sum, record) => sum + record.bytes, candidate.bytes) > limits.bytes) remove.push(others.shift().key);
  return { accepted: true, remove };
}

export function createIndexedNoteCacheStorage(factory = globalThis.indexedDB) {
  let opening;
  const database = () => {
    if (!opening) opening = new Promise((resolve, reject) => {
      if (!factory) { reject(failure()); return; }
      let rejected = false;
      const request = factory.open('soty-notes-cache-v1', 1);
      request.onupgradeneeded = () => {
        const records = request.result.createObjectStore('snapshots', { keyPath: 'key' }); records.createIndex('scope', 'scope');
        request.result.createObjectStore('meta');
      };
      request.onerror = request.onblocked = () => { rejected = true; reject(failure()); };
      request.onsuccess = () => {
        const db = request.result; if (rejected) { db.close(); return; }
        db.onversionchange = () => { db.close(); opening = undefined; }; resolve(db);
      };
    }).catch(error => { opening = undefined; throw error; });
    return opening;
  };
  async function transaction(mode, operate) {
    const db = await database();
    return new Promise((resolve, reject) => {
      let result; let cause;
      const tx = db.transaction(['snapshots', 'meta'], mode);
      const guard = callback => event => { try { callback(event); } catch (error) { cause = error; tx.abort(); } };
      try { operate(tx.objectStore('snapshots'), tx.objectStore('meta'), value => { result = value; }, guard); }
      catch (error) { cause = error; tx.abort(); }
      tx.oncomplete = () => resolve(result); tx.onabort = tx.onerror = () => reject(cause || failure());
    });
  }
  return {
    capture: () => transaction('readwrite', (_records, meta, done, guard) => {
      const request = meta.get('epoch'); request.onsuccess = guard(() => {
        const epoch = request.result || crypto.randomUUID(); if (!request.result) meta.put(epoch, 'epoch'); done(epoch);
      });
    }),
    read: (scope, noteId, token, now) => transaction('readwrite', (records, meta, done, guard) => {
      const epoch = meta.get('epoch'); epoch.onsuccess = guard(() => {
        if (token && epoch.result !== token) { done(null); return; }
        const request = records.get(noteKey(scope, noteId)); request.onsuccess = guard(() => {
          if (!request.result) { done(null); return; }
          const record = { ...request.result, usedAt: now }; records.put(record); done(record);
        });
      });
    }),
    list: (scope, token) => transaction('readonly', (records, meta, done, guard) => {
      const epoch = meta.get('epoch'); epoch.onsuccess = guard(() => {
        if (token && epoch.result !== token) { done([]); return; }
        const request = records.index('scope').getAll(scope, NOTE_CACHE_LIMITS.scopeCount); request.onsuccess = guard(() => done(request.result));
      });
    }),
    put: (record, token, limits) => transaction('readwrite', (records, meta, done, guard) => {
      const epoch = meta.get('epoch'); epoch.onsuccess = guard(() => {
        if (!token || epoch.result !== token) { done(false); return; }
        const request = records.getAll(undefined, NOTE_CACHE_LIMITS.count + 1); request.onsuccess = guard(() => {
          const decision = planNoteCacheWrite(request.result, record, limits);
          if (decision.accepted) { for (const key of decision.remove) records.delete(key); records.put(record); }
          done(decision.accepted);
        });
      });
    }),
    invalidate: (scope, noteId) => transaction('readwrite', (records, meta, done, guard) => {
      // One bounded durable fence invalidates all older in-flight cache writes.
      // It contains no account data and does not grow with deleted note IDs.
      meta.put(crypto.randomUUID(), 'epoch');
      if (noteId !== undefined) records.delete(noteKey(scope, noteId));
      else {
        const cursor = records.index('scope').openKeyCursor(scope); cursor.onsuccess = guard(() => {
          if (cursor.result) { records.delete(cursor.result.primaryKey); cursor.result.continue(); }
        });
      }
      done();
    }),
  };
}

export function createNoteCache(accountId, projectId = 'default', { origin = globalThis.location?.origin, storage = createIndexedNoteCacheStorage(), clock = Date.now, limits: overrides = {} } = {}) {
  if (![origin, projectId, accountId].every(value => typeof value === 'string' && value.length > 0 && value.length <= 512)) throw new Error('notes_cache_invalid_scope');
  const scope = JSON.stringify([origin, projectId, accountId]); const limits = { ...NOTE_CACHE_LIMITS, ...overrides }; let blocked = false;
  for (const [key, value] of Object.entries(limits)) if (!Object.hasOwn(NOTE_CACHE_LIMITS, key) || !Number.isSafeInteger(value) || value < 1 || value > NOTE_CACHE_LIMITS[key]) throw new Error('notes_cache_invalid_limits');
  const projection = record => record ? { note: validatedNote(record.note), verifiedAt: record.verifiedAt } : null;
  return {
    capture: () => storage.capture(),
    async isCurrent(token) {
      if (blocked) return false;
      let epoch;
      try { epoch = token ? await storage.capture() : null; }
      // A request which captured an epoch must prove it is still current.
      // remove() and another tab's clear() do not set this instance's blocked flag.
      catch { return false; }
      return !blocked && (!token || epoch === token);
    },
    async remember(note, token, verifiedAt = clock()) {
      if (blocked || !token) return false;
      const clean = validatedNote(note);
      const record = { key: noteKey(scope, clean.noteId), scope, note: clean, verifiedAt, usedAt: clock() }; const bytes = byteSize(record) + 32;
      return storage.put({ ...record, bytes }, token, limits);
    },
    async get(noteId, token) { if (blocked) return null; const record = await storage.read(scope, noteId, token, clock()); return blocked ? null : projection(record); },
    async list(token) {
      if (blocked) return [];
      const records = await storage.list(scope, token); if (blocked) return [];
      return records.map(projection).filter(Boolean).sort((a, b) => Number(b.note.pinned) - Number(a.note.pinned) || b.note.updatedAt - a.note.updatedAt || a.note.noteId.localeCompare(b.note.noteId));
    },
    async remove(noteId) { try { await storage.invalidate(scope, noteId); } catch (error) { blocked = true; throw error; } },
    async clear() { blocked = true; await storage.invalidate(scope); },
  };
}

export async function invalidateNoteCache(cache, error, noteId) {
  const policy = noteCacheErrorPolicy(error);
  try { if (policy === 'authorization') await cache.clear(); else if (policy === 'missing' && noteId) await cache.remove(noteId); }
  catch { /* A failed cache cleanup never changes the authoritative server outcome. */ }
  return policy;
}

/** Network-only recovery. The cache cannot manufacture authorization or revive a missing note. */
export async function readNoteWithCache({ api, accountId, noteId, cache, allowStale = true, active = () => true }) {
  const token = await cache.capture().catch(() => null);
  if (!active()) throw new Error('notes_account_changed');
  try {
    const result = await api.request('notes.get', { expectedAccountId: accountId, noteId });
    if (!active()) throw new Error('notes_account_changed');
    const note = validatedNote(result?.note); if (note.noteId !== noteId) throw new Error('notes_cache_invalid_snapshot');
    const verifiedAt = Date.now(); let cached = false;
    try { cached = await cache.remember(note, token, verifiedAt); } catch { /* Server result remains valid without a cache. */ }
    if (!active()) throw new Error('notes_account_changed');
    // A later authoritative denial/purge wins over this older successful read.
    // If cache storage was absent from the start, token=null is online-only.
    // Once a token was captured, losing the final fence cannot prove freshness.
    const current = await cache.isCurrent(token).catch(() => !token);
    if (!current) throw new Error('notes_cache_invalidated');
    if (!active()) throw new Error('notes_account_changed');
    return { note, source: 'server', verifiedAt, cached };
  } catch (error) {
    if (!active()) throw error;
    const policy = await invalidateNoteCache(cache, error, noteId);
    if (allowStale && policy === 'network' && token) {
      const snapshot = await cache.get(noteId, token).catch(() => null);
      if (active() && snapshot) return { ...snapshot, source: 'cache', cached: true };
    }
    throw error;
  }
}
