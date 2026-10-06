import { createFieldDocument, validateFieldDocument } from '../../modules/field/contract.mjs';
import type { FieldDocument } from '../../modules/field/contract.mjs';
import type { DirectoryApi } from './field-directory';

export interface FieldEnvelope { revision: number; document: FieldDocument; contentHash: string; updatedAt: number }
export interface FieldPending { requestId: string; expectedRevision: number; document: FieldDocument; contentHash: string }
export interface FieldLocalRecord { schema: 'soty.field.local.v1'; scope: string; localRevision: number; base: FieldEnvelope; pending: FieldPending[]; conflict: boolean; remote: FieldEnvelope | null; conflictDocument: FieldDocument | null }
export interface FieldLocalStore {
  read(): Promise<FieldLocalRecord | null>;
  write(record: FieldLocalRecord, expectedLocalRevision: number): Promise<void>;
  subscribe?(listener: () => void): () => void;
  dispose?(): void;
}
export interface FieldPersistenceState {
  document: FieldDocument; revision: number; projectedRevision: number; state: 'loading' | 'saved' | 'saving' | 'offline' | 'conflict' | 'storage-error';
  localDurable: boolean; pendingCount: number; errorCode?: string; remote?: FieldEnvelope;
}
export interface FieldCommitResult { status: 'saved' | 'volatile' | 'conflict'; revision: number; document: FieldDocument; errorCode?: string; localDurable: boolean }
const fail = (code: string): never => { throw Object.assign(new Error(code), { code }); };
const codeOf = (error: unknown): string => String((error as { code?: unknown })?.code ?? 'field_network_unavailable');
const clone = <T>(value: T): T => structuredClone(value);
const recordScope = (origin: string, project: string, account: string): string => JSON.stringify([origin, project, account]);
const sameDocument = (left: FieldDocument, right: FieldDocument): boolean => JSON.stringify(left) === JSON.stringify(right);
async function hashDocument(document: FieldDocument): Promise<string> {
  const hash = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(JSON.stringify(document)));
  return [...new Uint8Array(hash)].map(byte => byte.toString(16).padStart(2, '0')).join('');
}

/** Atomic account-local cache/outbox. Channel notices contain no document data. */
export function createIndexedDbFieldStore({ accountId, projectId = 'soty', origin = globalThis.location?.origin ?? 'local',
  indexedDB = globalThis.indexedDB }: { accountId: string; projectId?: string; origin?: string; indexedDB?: IDBFactory }): FieldLocalStore {
  const scope = recordScope(origin, projectId, accountId); let opening: Promise<IDBDatabase> | null = null, disposed = false;
  const listeners = new Set<() => void>();
  const channel = typeof BroadcastChannel === 'function' ? new BroadcastChannel('soty-field-local-v1') : null;
  const notify = (): void => { if (!disposed) for (const listener of listeners) listener(); };
  if (channel) channel.onmessage = event => { if (event.data?.scope === scope) notify(); };
  const openingDatabase = (): Promise<IDBDatabase> => {
    if (disposed || !indexedDB) return Promise.reject(Object.assign(new Error('field_local_storage_unavailable'), { code: 'field_local_storage_unavailable' }));
    if (!opening) opening = new Promise<IDBDatabase>((resolve, reject) => {
      const request = indexedDB.open('soty-field-local-v1', 1);
      request.onupgradeneeded = () => request.result.createObjectStore('documents', { keyPath: 'scope' });
      request.onerror = request.onblocked = () => reject(Object.assign(new Error('field_local_storage_unavailable'), { code: 'field_local_storage_unavailable' }));
      request.onsuccess = () => {
        const db = request.result;
        if (disposed) { db.close(); reject(Object.assign(new Error('field_account_changed'), { code: 'field_account_changed' })); return; }
        db.onversionchange = () => { db.close(); opening = null; }; resolve(db);
      };
    }).catch(error => { opening = null; throw error; });
    return opening;
  };
  return {
    async read() {
      const db = await openingDatabase();
      return new Promise<FieldLocalRecord | null>((resolve, reject) => {
        const tx = db.transaction('documents', 'readonly'), request = tx.objectStore('documents').get(scope); let result: FieldLocalRecord | null = null;
        request.onsuccess = () => { result = request.result ?? null; };
        tx.oncomplete = () => resolve(result); tx.onabort = tx.onerror = () => reject(Object.assign(new Error('field_local_storage_unavailable'), { code: 'field_local_storage_unavailable' }));
      });
    },
    async write(record, expectedLocalRevision) {
      if (record.scope !== scope || record.localRevision !== expectedLocalRevision + 1) fail('field_local_revision_conflict');
      const bytes = new TextEncoder().encode(JSON.stringify(record)).byteLength;
      if (record.pending.length > 32 || bytes > 4 * 1024 * 1024) fail('field_local_capacity');
      const db = await openingDatabase();
      await new Promise<void>((resolve, reject) => {
        const tx = db.transaction('documents', 'readwrite'), store = tx.objectStore('documents'), request = store.get(scope); let errorCode = '';
        request.onsuccess = () => {
          if (disposed || (request.result?.localRevision ?? 0) !== expectedLocalRevision) { errorCode = disposed ? 'field_account_changed' : 'field_local_revision_conflict'; tx.abort(); return; }
          store.put(clone(record));
        };
        tx.oncomplete = () => resolve(); tx.onabort = tx.onerror = () => reject(Object.assign(new Error(errorCode || 'field_local_storage_unavailable'), { code: errorCode || 'field_local_storage_unavailable' }));
      });
      channel?.postMessage({ scope, revision: record.localRevision }); notify();
    },
    subscribe(listener) { listeners.add(listener); return () => { listeners.delete(listener); }; },
    dispose() { disposed = true; channel?.close(); listeners.clear(); void opening?.then(db => db.close()).catch(() => {}); },
  };
}

/** A durable intent precedes network dispatch; retries keep its identity and CAS. */
export function createFieldPersistence({ api, accountId, projectId = 'soty', isCurrent = () => true, onChange, store: suppliedStore, localFirst = false }:
  { api: DirectoryApi; accountId: string; projectId?: string; isCurrent?: () => boolean; onChange?: (state: FieldPersistenceState) => void; store?: FieldLocalStore; localFirst?: boolean }) {
  const origin = globalThis.location?.origin ?? 'local';
  const scope = recordScope(origin, projectId, accountId), store = suppliedStore ?? createIndexedDbFieldStore({ accountId, projectId, origin });
  const listeners = new Set<(state: FieldPersistenceState) => void>(); if (onChange) listeners.add(onChange);
  const ownedIntents = new Set<string>();
  let local: FieldLocalRecord | null = null, disposed = false, loaded = false, saving = false, volatileDocument: FieldDocument | null = null;
  let volatileIntent: FieldPending | null = null;
  let errorCode = '', storageError = false, queue: Promise<unknown> = Promise.resolve(), drainFlight: Promise<void> | null = null;
  const check = (): void => { if (disposed || !isCurrent()) fail('field_account_changed'); };
  const document = (): FieldDocument => clone(volatileDocument ?? local?.conflictDocument ?? local?.pending.at(-1)?.document ?? local?.base.document ?? createFieldDocument());
  function snapshot(): FieldPersistenceState {
    return { document: document(), revision: local?.base.revision ?? 0, projectedRevision: (local?.base.revision ?? 0) + (local?.pending.length ?? 0),
      state: !loaded ? 'loading' : storageError ? 'storage-error' : local?.conflict ? 'conflict' : saving ? 'saving' : local?.pending.length || errorCode ? 'offline' : 'saved',
      localDurable: loaded && !volatileDocument && !volatileIntent
        && (!storageError || (local?.localRevision ?? 0) > 0), pendingCount: local?.pending.length ?? 0,
      ...(errorCode ? { errorCode } : {}), ...(local?.remote ? { remote: clone(local.remote) } : {}) };
  }
  function emit(): void { if (!disposed && isCurrent()) { const state = snapshot(); for (const listener of listeners) listener(state); } }
  const serialize = <T>(action: () => Promise<T>): Promise<T> => {
    const next = queue.catch(() => {}).then(action); queue = next; return next;
  };
  async function envelope(value: FieldEnvelope): Promise<FieldEnvelope> {
    if (!value || !Number.isSafeInteger(value.revision) || value.revision < 0 || !Number.isSafeInteger(value.updatedAt) || value.updatedAt < 0) fail('field_invalid_receipt');
    const normalized = validateFieldDocument(value.document);
    if (typeof value.contentHash !== 'string' || await hashDocument(normalized) !== value.contentHash) fail('field_invalid_receipt');
    check(); return { revision: value.revision, updatedAt: value.updatedAt, contentHash: value.contentHash, document: normalized };
  }
  async function empty(): Promise<FieldLocalRecord> {
    const normalized = createFieldDocument();
    return { schema: 'soty.field.local.v1', scope, localRevision: 0, base: { revision: 0, updatedAt: 0, contentHash: await hashDocument(normalized), document: normalized }, pending: [], conflict: false, remote: null, conflictDocument: null };
  }
  async function validateLocal(value: FieldLocalRecord): Promise<FieldLocalRecord> {
    if (value?.schema !== 'soty.field.local.v1' || value.scope !== scope || !Number.isSafeInteger(value.localRevision) || value.localRevision < 1
      || !Array.isArray(value.pending) || value.pending.length > 32 || typeof value.conflict !== 'boolean'
      || new TextEncoder().encode(JSON.stringify(value)).byteLength > 4 * 1024 * 1024) fail('field_local_storage_corrupt');
    const base = await envelope(value.base), pending: FieldPending[] = [], ids = new Set<string>();
    for (const intent of value.pending) {
      const normalized = validateFieldDocument(intent.document);
      if (typeof intent.requestId !== 'string' || !/^[A-Za-z0-9_-]{3,160}$/u.test(intent.requestId) || ids.has(intent.requestId)
        || !Number.isSafeInteger(intent.expectedRevision) || intent.expectedRevision < 0 || await hashDocument(normalized) !== intent.contentHash) fail('field_local_storage_corrupt');
      ids.add(intent.requestId); pending.push({ requestId: intent.requestId, expectedRevision: intent.expectedRevision, contentHash: intent.contentHash, document: normalized });
    }
    const remote = value.remote === null ? null : await envelope(value.remote); check();
    const conflictDocument = value.conflictDocument ? validateFieldDocument(value.conflictDocument) : null;
    return { schema: 'soty.field.local.v1', scope, localRevision: value.localRevision, base, pending, conflict: value.conflict, remote, conflictDocument };
  }
  async function persist(next: FieldLocalRecord): Promise<void> {
    check(); const expected = local?.localRevision ?? 0;
    const detached = { ...clone(next), localRevision: expected + 1 };
    try {
      await store.write(detached, expected); check(); local = detached; storageError = false;
      const retained = new Set(detached.pending.map(intent => intent.requestId));
      for (const requestId of ownedIntents) if (!retained.has(requestId)) ownedIntents.delete(requestId);
    }
    catch (error) {
      errorCode = codeOf(error); storageError = errorCode !== 'field_local_revision_conflict';
      if (!storageError && local) local.conflict = true;
      emit(); throw error;
    }
  }
  const remoteRead = async (): Promise<FieldEnvelope> => {
    check(); const response = await api.request<FieldEnvelope>('world.field.get', { expectedAccountId: accountId }); check(); return envelope(response);
  };
  async function applyRemote(remote: FieldEnvelope, capturedRevision: number): Promise<void> {
    check();
    if (!local) local = await empty();
    if (remote.revision < local.base.revision) {
      // This GET may have started before a verified in-flight PUT advanced our
      // base. Its earlier snapshot cannot replace that acknowledged document.
      if (local.base.revision > capturedRevision) return;
      fail('field_invalid_receipt');
    }
    if (remote.revision === local.base.revision && remote.contentHash !== local.base.contentHash) fail('field_invalid_receipt');
    if (local.pending.length || local.conflict) {
      // A request may already have committed before its ACK was lost. Do not
      // infer conflict from GET alone: replay the original receipt first.
      if (remote.revision > local.base.revision) local.remote = remote;
      return;
    }
    const next = { ...clone(local), base: remote, conflict: false, remote: null, conflictDocument: null };
    await persist(next); errorCode = ''; volatileDocument = null;
  }
  async function refreshRemote(): Promise<void> {
    const capturedRevision = await serialize(async () => { check(); return local?.base.revision ?? 0; });
    const remote = await remoteRead();
    await serialize(() => applyRemote(remote, capturedRevision));
  }
  async function acceptReceipt(intent: FieldPending, current: FieldEnvelope, acceptedRevision: number): Promise<void> {
    // Two tabs may drain one shared durable outbox or append another edit while
    // the network is awaiting an ACK. Merge only the proven receipt, never a
    // whole older local snapshot over a newer pending queue.
    for (let attempt = 0; attempt < 3; attempt++) {
      check(); if (!local) fail('field_local_revision_conflict');
      const next = clone(local!);
      if (next.pending[0]?.requestId !== intent.requestId || next.pending[0]?.contentHash !== intent.contentHash) {
        if (next.base.revision >= acceptedRevision && !next.pending.some(value => value.requestId === intent.requestId)) return;
        fail('field_local_revision_conflict');
      }
      next.pending.shift(); next.base = current; next.remote = null;
      next.conflict = current.revision > acceptedRevision && current.contentHash !== intent.contentHash;
      if (next.conflict) { next.remote = current; next.conflictDocument = next.pending.at(-1)?.document ?? intent.document; }
      else next.conflictDocument = null;
      try { await persist(next); return; }
      catch (error) {
        if (codeOf(error) !== 'field_local_revision_conflict') throw error;
        const latest = await store.read(); check(); if (!latest) throw error;
        local = await validateLocal(latest);
      }
    }
    fail('field_local_revision_conflict');
  }
  async function runDrain(): Promise<void> {
    check(); if (!loaded || storageError || local?.conflict || !local?.pending.length) return;
    saving = true; emit();
    try {
      while (true) {
        const intent = await serialize(async () => { check(); return !storageError && !local?.conflict && local?.pending.length ? clone(local.pending[0]!) : null; });
        if (!intent) break;
        const response = await api.request<{ replayed: boolean; receipt: { revision: number; contentHash: string; committedAt: number }; current: FieldEnvelope }>(
          'world.field.put', { expectedAccountId: accountId, ...intent });
        check(); const current = await envelope(response.current);
        if (typeof response.replayed !== 'boolean' || response.receipt?.revision !== intent.expectedRevision + 1
          || response.receipt.contentHash !== intent.contentHash || !Number.isSafeInteger(response.receipt.committedAt)
          || response.receipt.committedAt < 0 || current.revision < response.receipt.revision
          || current.revision === response.receipt.revision && current.contentHash !== intent.contentHash) fail('field_invalid_receipt');
        await serialize(async () => {
          await acceptReceipt(intent, current, response.receipt.revision);
          errorCode = local?.conflict ? 'field_revision_conflict' : ''; emit();
        });
      }
    } catch (error) {
      check(); const failedCode = codeOf(error);
      if (['field_revision_conflict', 'field_request_conflict', 'field_local_revision_conflict'].includes(failedCode)) {
        let remote = local?.remote ?? null;
        try { remote = await remoteRead(); } catch { /* Preserve the original durable intent and primary conflict. */ }
        await serialize(async () => { check(); errorCode = failedCode; if (local) {
          if (failedCode === 'field_local_revision_conflict') {
            const latest = await store.read(); check(); if (latest) { local = await validateLocal(latest); }
          }
          const next = { ...clone(local), conflict: true, remote }; await persist(next).catch(() => {});
        } });
      } else await serialize(async () => { check(); errorCode = failedCode; });
    } finally { saving = false; emit(); }
  }
  function drain(): Promise<void> {
    if (!drainFlight) drainFlight = runDrain().finally(() => { drainFlight = null; });
    return drainFlight;
  }
  const unobserve = store.subscribe?.(() => {
    if (disposed || !isCurrent()) return;
    void serialize(async () => {
      const latest = await store.read(); check();
      if (!latest || latest.localRevision === local?.localRevision) return;
      const next = await validateLocal(latest);
      // Merely displaying the shared outbox does not make this window its
      // author. A read-only observer may follow its validated latest head/ACK.
      if (local && (local.conflict || local.conflictDocument || local.pending.some(intent => ownedIntents.has(intent.requestId)) || volatileDocument || volatileIntent)) {
        const desired = document();
        const retained = [next.base.document, next.conflictDocument, ...next.pending.map(intent => intent.document)];
        // A different window can explicitly resolve the shared conflict. This
        // window still owes its user a choice; if the newer durable head no
        // longer retains those bytes, describe this buffer as undurable.
        if (!retained.some(value => value && sameDocument(value, desired))) volatileDocument = desired;
        errorCode = 'field_local_revision_conflict'; local.conflict = true;
      } else { local = next; errorCode = next.conflict ? 'field_revision_conflict' : ''; }
      emit();
    }).catch(error => { if (!disposed && isCurrent()) { errorCode = codeOf(error); storageError = true; emit(); } });
  }) ?? (() => {});
  return Object.freeze({
    async load(): Promise<FieldPersistenceState> {
      await serialize(async () => {
        check();
        try { const saved = await store.read(); check(); local = saved ? await validateLocal(saved) : await empty(); loaded = !!saved; }
        catch (error) { check(); storageError = true; errorCode = codeOf(error); local = await empty(); }
        emit();
      });
      try {
        if (storageError) {
          const remote = await remoteRead();
          await serialize(async () => { check(); local!.base = remote; });
        } else await refreshRemote();
      } catch (error) { check(); errorCode = codeOf(error); }
      await serialize(async () => { check(); loaded = true; emit(); });
      await drain(); return snapshot();
    },
    async commit(value: FieldDocument, options: { expectedRevision: number; requestId: string }): Promise<FieldCommitResult> {
      const normalized = validateFieldDocument(value), contentHash = await hashDocument(normalized); check();
      if (!Number.isSafeInteger(options.expectedRevision) || options.expectedRevision < 0 || !/^[A-Za-z0-9_-]{3,160}$/u.test(options.requestId)) fail('field_invalid_intent');
      const stored = await serialize(async () => {
        check(); if (!loaded || !local) fail('field_not_loaded');
        const current = local!;
        const intent = { requestId: options.requestId, expectedRevision: current.base.revision + current.pending.length, document: normalized, contentHash };
        if (storageError) { volatileDocument = normalized; volatileIntent = intent; emit(); fail(errorCode || 'field_local_storage_unavailable'); }
        const existing = current.pending.find(intent => intent.requestId === options.requestId);
        if (existing) {
          if (existing.contentHash !== contentHash || localFirst && existing.expectedRevision !== options.expectedRevision) fail('field_request_conflict');
          if (!current.conflict) { ownedIntents.add(existing.requestId); return true; }
        }
        if (current.conflict || options.expectedRevision !== current.base.revision + (localFirst ? current.pending.length : 0)) {
          volatileDocument = normalized; volatileIntent = intent; errorCode = 'field_revision_conflict'; current.conflict = true; emit(); return false;
        }
        const next = clone(current);
        next.pending.push(intent);
        volatileDocument = normalized; volatileIntent = intent;
        await persist(next); ownedIntents.add(intent.requestId); volatileDocument = null; volatileIntent = null; errorCode = ''; emit(); return true;
      });
      if (stored) {
        if (localFirst) void drain().catch(() => {});
        else await drain();
      }
      const state = snapshot();
      return { status: state.state === 'conflict' ? 'conflict' : state.pendingCount || state.state === 'storage-error' ? 'volatile' : 'saved',
        revision: localFirst && state.state !== 'conflict' ? state.projectedRevision : state.revision,
        document: state.state === 'conflict' && state.remote ? state.remote.document : state.document,
        localDurable: state.localDurable, ...(state.errorCode ? { errorCode: state.errorCode } : {}) };
    },
    retry: async (): Promise<FieldPersistenceState> => {
      check();
      if (storageError && !volatileIntent) {
        try {
          await serialize(async () => { check(); const latest = await store.read(); check(); local = latest ? await validateLocal(latest) : await empty(); storageError = false; });
          await refreshRemote(); emit();
        } catch (error) { check(); await serialize(async () => { errorCode = codeOf(error); storageError = true; emit(); }); }
      }
      if (volatileIntent && !local?.conflict) await serialize(async () => {
        check(); const latest = await store.read(); check();
        if ((latest?.localRevision ?? 0) !== (local?.localRevision ?? 0)) {
          if (local) local.conflict = true; errorCode = 'field_local_revision_conflict'; emit(); return;
        }
        if (!local) local = await empty(); const next = clone(local);
        if (!next.pending.some(intent => intent.requestId === volatileIntent!.requestId)) next.pending.push(clone(volatileIntent!));
        const ownedRequestId = volatileIntent!.requestId;
        await persist(next); ownedIntents.add(ownedRequestId); volatileDocument = null; volatileIntent = null; errorCode = ''; emit();
      });
      await drain(); return snapshot();
    },
    async resolveConflict(choice: 'local' | 'remote'): Promise<FieldPersistenceState> {
      if (!['local', 'remote'].includes(choice)) fail('field_invalid_intent');
      const remote = await remoteRead();
      await serialize(async () => {
        check(); const desired = document(), displayedLocalRevision = local?.localRevision ?? 0, latest = await store.read(); check();
        if (latest) local = await validateLocal(latest); else local = await empty();
        if (remote.revision < local!.base.revision || remote.revision === local!.base.revision && remote.contentHash !== local!.base.contentHash) fail('field_invalid_receipt');
        if (latest && latest.localRevision !== displayedLocalRevision && local!.pending.length) {
          // A different window owns durable edits that have not yet received
          // their receipts. Choosing its version must not erase that outbox.
          if (choice === 'remote') {
            volatileDocument = null; volatileIntent = null; errorCode = local!.conflict ? 'field_revision_conflict' : ''; emit(); return;
          }
          const next = clone(local!);
          next.pending.push({ requestId: crypto.randomUUID(), expectedRevision: next.pending.at(-1)!.expectedRevision + 1,
            document: desired, contentHash: await hashDocument(desired) });
          await persist(next); ownedIntents.add(next.pending.at(-1)!.requestId); volatileDocument = null; volatileIntent = null; errorCode = ''; emit(); return;
        }
        const next = clone(local); next.base = remote; next.pending = []; next.conflict = false; next.remote = null; next.conflictDocument = null;
        if (choice === 'local' && !sameDocument(desired, remote.document)) {
          next.pending.push({ requestId: crypto.randomUUID(), expectedRevision: remote.revision, document: desired, contentHash: await hashDocument(desired) });
        }
        await persist(next); if (next.pending.length) ownedIntents.add(next.pending.at(-1)!.requestId);
        errorCode = ''; volatileDocument = null; volatileIntent = null; emit();
      });
      await drain(); return snapshot();
    },
    async discardVolatile(): Promise<FieldPersistenceState> {
      return serialize(async () => {
        check();
        try { const latest = await store.read(); check(); if (latest) local = await validateLocal(latest); }
        catch (error) { check(); errorCode = codeOf(error); storageError = true; }
        // Only this controller's undurable buffer is discarded. The latest
        // shared outbox is neither removed nor rewritten, even when quota fails.
        volatileDocument = null; volatileIntent = null; emit(); return snapshot();
      });
    },
    subscribe(listener: (state: FieldPersistenceState) => void): () => void { listeners.add(listener); return () => { listeners.delete(listener); }; },
    getState: (): FieldPersistenceState => { check(); return snapshot(); },
    hasUnsavedChanges: (): boolean => !loaded || volatileDocument !== null || volatileIntent !== null,
    flush: async (): Promise<FieldPersistenceState> => { check(); await queue.catch(() => {}); await drain(); return snapshot(); },
    exportPending: () => { check(); return { schema: 'soty.field.pending-export.v1', accountId, document: document(), pending: clone(local?.pending ?? []),
      volatileIntent: clone(volatileIntent), baseRevision: local?.base.revision ?? 0 }; },
    dispose(): void { disposed = true; unobserve(); store.dispose?.(); listeners.clear(); },
  });
}
